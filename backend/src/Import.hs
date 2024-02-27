{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

module Import where

import Common
import Control.Arrow ((&&&))
import Control.Category ((>>>))
import Control.Monad (guard)
import Control.Monad.State (evalState, gets, modify)
import qualified Data.Aeson as Json
import Data.Aeson.Types ((.:), FromJSON(..), withObject)
import qualified Data.Aeson.Types as Json
import Data.Bifunctor (first)
import qualified Data.ByteString.Lazy as LBS
import Data.Char (isAlphaNum, isSpace)
import Data.FileEmbed (embedFile)
import Data.Foldable (asum, for_)
import Data.Function ((&))
import Data.Functor ((<&>), void)
import Data.GraphViz (DotGraph)
import Data.GraphViz.Attributes.Complete (Attribute (..), Label (..))
import qualified Data.GraphViz.Parsing as GraphViz
import Data.GraphViz.Types (DotEdge (..))
import qualified Data.GraphViz.Types as GraphViz
import Data.List (sortOn)
import Data.List.NonEmpty (NonEmpty (..))
import qualified Data.List.NonEmpty as NonEmpty
import qualified Data.Map as Map
import Data.Maybe (fromMaybe, mapMaybe)
import Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Data.Text.IO as Text
import qualified Data.Text.Lazy as LazyText
import qualified Data.Text.Lazy.IO as LazyText
import Data.Traversable (for)
import Data.Tuple (swap)
import Database.SQLite3 (SQLData (..))
import Foreign.C.String (CString, withCString)
import GHC.Stack (HasCallStack)
import Model
import Prelude
import qualified Sqlite
import Sqlite (Database)
import System.IO (Handle)

withDatabase :: String -> FilePath -> (Database -> IO a) -> IO a
withDatabase label path action = timed label $
  Sqlite.withDatabase path $ \database -> do
    putStrLn label
    createSchema database
    Sqlite.executeStatements
      database
      [ ["pragma synchronous = off;"],
        ["pragma journal_mode = MEMORY;"],
        ["pragma mmap_size = 1073741824;"]
      ]
    action database
  where
    createSchema :: Database -> IO ()
    createSchema database = do
      let sql = Text.decodeUtf8 $(embedFile "backend/src/schema.sql")
      Sqlite.executeStatements database $ Text.lines <$> Text.splitOn "\n\n" sql

importSkyframe :: FilePath -> IO ()
importSkyframe path =
  withDatabase "importing skyframe" path $ \database -> do
    guard =<< withCString path c_importSkyframe
    extractContextKeys database

foreign import ccall safe "import.cpp"
  c_importSkyframe :: CString -> IO Bool

extractContextKeys :: Database -> IO ()
extractContextKeys =
  Sqlite.executeStatement
    [ "UPDATE node SET context_key = CASE",
      when "CONFIGURED_TARGET" label,
      when "ACTION_EXECUTION" $ label <> " || ' ' || " <> actionIndex,
      when "BUILD_CONFIGURATION" config,
      "ELSE NULL END"
    ]
  where
    when nodeType expr = "WHEN glob('" <> nodeType <> ":*', data) THEN " <> expr
    label = extract "label=" {- //some/target:label -} ", config="
    actionIndex = extract "actionIndex=" {- 0 -} "}"
    config = extract "BuildConfigurationKey[" "]"
    extract prefix suffix =
      let i = "INSTR(data, '" <> prefix <> "')"
          j = "INSTR(SUBSTR(data, " <> i <> "),'" <> suffix <> "')"
       in Text.pack $
            unlines
              [ "SUBSTR(data, ",
                i <> " + " <> show (length prefix) <> ", ",
                j <> " - " <> show (length prefix + 1),
                ")"
              ]

addContext :: Database -> [(Text, Text)] -> IO ()
addContext database context = do
  let records = context <&> \(k, v) -> [SQLText k, SQLText v]
  Sqlite.batchInsert database "context" ["context_key", "context_data"] records

listConfigs :: Handle -> FilePath -> IO [Text]
listConfigs source _ =
  Text.lines <$> Text.hGetContents source <&> \case
    "Available configurations:" : lines -> head . Text.words <$> lines
    _ -> error "unexpected output from bazel config"

importConfig :: Text -> Handle -> FilePath -> IO ()
importConfig hash source path = withDatabase "importing config " path $ \database -> do
  config <- Text.hGetContents source
  void $
    Sqlite.executeSql
      database
      ["INSERT INTO context (context_key, context_data) VALUES (?, ?);"]
      [SQLText hash, SQLText config]

importTargets :: HasCallStack => Handle -> FilePath -> IO ()
importTargets source path = withDatabase "importing targets" path $ \database -> do
  workspace <- maybe "" Text.pack <$> getSkyscopeEnv "WORKSPACE"
  external <- maybe "" Text.pack <$> getSkyscopeEnv "OUTPUT_BASE" <&> (<> "/external/")

  let parseRepository text = case Text.stripPrefix ("# " <> workspace) text of
        Just text -> ("/", text)
        Nothing -> case Text.stripPrefix ("# " <> external) text of
          Just text -> first (("@" <>) >>> (<> "/")) (Text.breakOn "/" text)
          Nothing -> errorRed $ "failed to parse target label:\n" <> Text.unpack text

      parsePackage text =
        parseRepository text & \(repo, text) -> case Text.stripPrefix "/BUILD" text of
          Just _ -> (repo <> "/", text)
          Nothing -> first (repo <>) (Text.breakOn "/BUILD" text)

      parseLabel text =
        parsePackage text & \(package, text) ->
          findLine "  name = \"" text
            <&> (Text.break (== '"') >>> \(name, _) -> package <> ":" <> name)

      parseParagraphs =
        mapMaybe (fmap swap . sequenceA . (id &&& parseLabel))

  targets <- parseParagraphs <$> getParagraphs source (Text.any (/= '\n'))
  for_ targets $ \(label, _) -> putStrLn $ "importing target " <> Text.unpack label
  addContext database targets

importActions :: HasCallStack => Handle -> FilePath -> IO ()
importActions source path = withDatabase "importing actions" path $ \database -> do
  let parseAction paragraph = swap <$> sequenceA (paragraph, findLine "  Target: " paragraph)
      indexedActions group = (0 :| [1 ..]) `NonEmpty.zip` (snd <$> group)
      filterActions = filter $ \paragraph ->
        let stripValidPrefix =
              fmap asum $
                sequenceA
                  [ Text.stripPrefix "action '",
                    Text.stripPrefix "for ",
                    Text.stripPrefix "BazelCppSemantics"
                  ]
         in case stripValidPrefix paragraph of
              Just _ -> True
              Nothing ->
                let dropFirstWord = Text.unwords . drop 1 . Text.words
                 in case stripValidPrefix $ dropFirstWord paragraph of
                      Just _ -> True
                      Nothing
                        | Text.all isSpace paragraph -> False
                        | otherwise -> False -- errorRed $ "failed to parse action:\n" <> Text.unpack paragraph
  groups <-
    NonEmpty.groupBy (\x y -> fst x == fst y)
      . sortOn fst
      . mapMaybe parseAction
      . filterActions
      <$> getParagraphs source (const True)

  let actions =
        concat $
          groups <&> \group@((label, _) :| _) ->
            NonEmpty.toList $
              indexedActions group <&> \(index, contextData) ->
                let contextKey = label <> " " <> Text.pack (show @Integer index)
                 in (contextKey, contextData)
  for_ actions $ \(label, _) -> putStrLn $ "importing action " <> Text.unpack label
  addContext database actions

getParagraphs :: Handle -> (Text -> Bool) -> IO [Text]
getParagraphs source predicate = filter predicate . Text.splitOn "\n\n" <$> Text.hGetContents source

findLine :: HasCallStack => Text -> Text -> Maybe Text
findLine prefix paragraph =
  let find = \case
        [] -> Nothing
        (line : lines) -> case Text.stripPrefix prefix line of
          Nothing -> find lines
          result -> result
   in find $ Text.lines paragraph

    {-
        {
          "bom-ref": "/nix/store/zzrp853p377h0344yjbm9qmmvk4gd9dv-readline-8.2p10-dev",
          "description": "Library for interactive line editing",
          "externalReferences": [
            {
              "type": "website",
              "url": "https://savannah.gnu.org/projects/readline/"
            }
          ],
          "licenses": [
            {
              "license": {
                "id": "GPL-3.0-or-later"
              }
            }
          ],
          "name": "readline",
          "purl": "pkg:generic/readline@8.2p10",
          "type": "application",
          "version": "8.2p10"
        }
    -}

data CycloneComponent = CycloneComponent
  { cycloneComponent :: Json.Object
  , cycloneBomRef :: Text
  , cycloneDescription :: Text
  , cycloneName :: Text
  , cycloneVersion :: Text
  } deriving Show

instance FromJSON CycloneComponent where
  parseJSON = withObject "CycloneComponent" $ \o -> do
    let cycloneComponent = o
    cycloneBomRef <- o .: "bom-ref"
    cycloneDescription <- o .: "description"
    cycloneName <- o .: "name"
    cycloneVersion <- o .: "version"
    pure CycloneComponent {..}

importCyclone :: Handle -> FilePath -> IO ()
importCyclone source path = withDatabase "importing cyclone" path $ \database -> do
  Json.decode @CycloneComponent <$> LBS.hGetContents source
  pure ()

importGraphviz :: Handle -> FilePath -> IO ()
importGraphviz source path = withDatabase "importing graphviz" path $ \database -> do
  dotGraph <- GraphViz.parseIt' @(DotGraph Text) <$> LazyText.hGetContents source
  let (nodes, edges) = (GraphViz.nodeInformation False &&& GraphViz.graphEdges) dotGraph
  importGeneric path database (Map.fromList $ convertNode <$> Map.assocs nodes) (convertEdge <$> edges)
  where
    convertEdge DotEdge {..} = (fromNode, toNode)
    convertNode (nodeID, (_, attrs)) = (nodeID, getLabel nodeID attrs)
    getLabel nodeID attrs = case attrs of
      Label (StrLabel label) : _ -> LazyText.toStrict label
      _ : attrs' -> getLabel nodeID attrs'
      [] -> nodeID

importGeneric :: FilePath -> Database -> NodeMap Text -> [(NodeHash, NodeHash)] -> IO ()
importGeneric path database nodes edges = do
  let assignIndex node = gets (,node) <* modify (+ 1)
      indexedNodes = evalState (for nodes assignIndex) 1
      coerceMaybe = fromMaybe $ errorRed "parser produced an edge with an unknown node"
      indexedEdges = coerceMaybe $
        for edges $ \(fromNode, toNode) -> do
          sourceIndex <- fst <$> Map.lookup fromNode indexedNodes
          targetIndex <- fst <$> Map.lookup toNode indexedNodes
          pure (0, sourceIndex, targetIndex)
  Sqlite.batchInsert database "node" ["idx", "hash", "data", "type"] $
    Map.assocs indexedNodes <&> \(nodeID, (nodeIdx, label)) ->
      let (nodeData, nodeType) = case Text.splitOn "\\n" label of
            nodeType : rest@(_ : _) ->
              let allowed c = isAlphaNum c || c == '_'
                  nodeType' = Text.filter allowed nodeType
                  nodeData' = Text.intercalate " " $ nodeType' : rest
               in (nodeData', nodeType')
            [nodeData] -> (nodeData, nodeID)
            _ -> errorRed "unexpected graphviz node label"
       in SQLInteger nodeIdx : (SQLText <$> [nodeID, nodeData, nodeType])
  Sqlite.batchInsert database "edge" ["group_num", "source", "target"] $
    indexedEdges <&> \(g, s, t) -> SQLInteger <$> [g, s, t]
  putStrLn $ path <> "\n    imported " <> show (Map.size nodes) <> " nodes and " <> show (length edges) <> " edges"

errorRed :: HasCallStack => String -> a
errorRed message = error $ "\x1b[31m" <> message <> "\x1b[0m"

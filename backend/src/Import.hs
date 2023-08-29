{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE RecordWildCards #-}

module Import where

import Common
import Control.Arrow ((&&&))
import Control.Category ((>>>))
import Control.Monad (guard)
import Data.Traversable (for)
import Control.Monad.State (gets, modify, evalState)
import Data.Bifunctor (first)
import Control.Concurrent (threadDelay)
import Data.FileEmbed (embedFile)
import Data.Foldable (asum, for_)
import Data.Function ((&))
import Data.Functor ((<&>))
import Data.GraphViz (DotGraph)
import qualified Data.GraphViz.Parsing as GraphViz
import qualified Data.GraphViz.Types as GraphViz
import Data.GraphViz.Parsing (parseIt')
import Data.GraphViz.Types (graphNodes, graphEdges, DotNode(..), DotEdge(..))
import Data.GraphViz.Attributes.Complete (Attribute(..), Label(..))
import Data.List (sortOn)
import Data.List.NonEmpty (NonEmpty (..))
import qualified Data.List.NonEmpty as NonEmpty
import Data.Maybe (isJust, fromMaybe)
import Data.Text (Text)
import qualified Data.Map as Map
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Data.Text.IO as Text
import qualified Data.Text.Lazy as LazyText
import qualified Data.Text.Lazy.IO as LazyText
import Database.SQLite3 (SQLData (..))
import Foreign.C.String (CString, withCString)
import Sqlite (Database)
import qualified Sqlite
import System.IO (Handle)
import Prelude

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

addContext :: Database -> [(Text, Text)] -> IO ()
addContext database context = do
  let records = context <&> \(k, v) -> [SQLText k, SQLText v]
  Sqlite.batchInsert database "context" ["context_key", "context_data"] records

importSkyframe :: FilePath -> IO ()
importSkyframe path =
  withDatabase "importing skyframe" path $
    const $ -- withDatabase creates the schema
      guard =<< withCString path c_importSkyframe

foreign import ccall safe "import.cpp"
  c_importSkyframe :: CString -> IO Bool

importTargets :: Handle -> FilePath -> IO ()
importTargets source path = withDatabase "importing targets" path $ \database -> do
  workspace <- maybe "" Text.pack <$> getSkyscopeEnv "WORKSPACE"
  external <- maybe "" Text.pack <$> getSkyscopeEnv "OUTPUT_BASE" <&> (<> "/external/")
  let parseRepository text = case Text.stripPrefix ("# " <> workspace) text of
        Just text -> ("/", text)
        Nothing -> case Text.stripPrefix ("# " <> external) text of
          Just text -> first (("@" <>) >>> (<> "/")) (Text.breakOn "/" text)
          Nothing -> error $ "failed to parse target label:\n" <> Text.unpack text
      parsePackage text =
        parseRepository text & \(repo, text) -> case Text.stripPrefix "/BUILD" text of
          Just _ -> (repo <> "/", text)
          Nothing -> first (repo <>) (Text.breakOn "/BUILD" text)
      parseLabel text =
        parsePackage text & \(package, text) ->
          ((package <> ":") <>) $ fst $ findLine "  name = \"" text & Text.break (== '"')
  targets <- map (parseLabel &&& id) <$> getParagraphs source
  addContext database targets

importActions :: Handle -> FilePath -> IO ()
importActions source path = withDatabase "importing actions" path $ \database -> do
  let parseAction paragraph = (findLine "  Target: " paragraph, paragraph)
      indexedActions group = (0 :| [1 ..]) `NonEmpty.zip` (snd <$> group)
      filterActions = filter $ \paragraph ->
        isJust $
          asum
            [ Text.stripPrefix "action " paragraph,
              Text.stripPrefix "runfiles " paragraph,
              Text.stripPrefix "BazelCppSemantics" paragraph
            ]
  groups <-
    NonEmpty.groupBy (\x y -> fst x == fst y)
      . sortOn fst
      . map parseAction
      . filterActions
      <$> getParagraphs source
  addContext database $
    concat $
      groups <&> \group@((label, _) :| _) ->
        NonEmpty.toList $
          indexedActions group <&> \(index, contextData) ->
            let contextKey = label <> " " <> Text.pack (show @Integer index)
             in (contextKey, contextData)

getParagraphs :: Handle -> IO [Text]
getParagraphs source = filter (Text.any (/= '\n')) . Text.splitOn "\n\n" <$> Text.hGetContents source

findLine :: Text -> Text -> Text
findLine prefix paragraph =
  let find = \case
        [] -> error $ "missing " <> Text.unpack prefix <> ":\n" <> Text.unpack paragraph
        (line : lines) -> case Text.stripPrefix prefix line of
          Nothing -> find lines
          Just text -> text
   in find $ Text.lines paragraph

importGraphviz :: Handle -> FilePath -> IO ()
importGraphviz source path = withDatabase "importing graphviz" path $ \database -> do
  dotGraph <- GraphViz.parseIt' @(DotGraph Text) <$> LazyText.hGetContents source
  let (nodes, edges) = (GraphViz.nodeInformation False &&& GraphViz.graphEdges) dotGraph
  let assignIndex node = gets (,node) <* modify (+ 1)
      indexedNodes = evalState (for nodes assignIndex) 1
      coerceMaybe = fromMaybe $ error "parser produced an edge with an unknown node"
      indexedEdges = coerceMaybe $
        for edges $ \DotEdge{..} -> do
          sourceIndex <- fst <$> Map.lookup fromNode indexedNodes
          targetIndex <- fst <$> Map.lookup toNode indexedNodes
          pure (0, sourceIndex, targetIndex)
  for indexedEdges $ \x -> putStrLn $ show x


{-
    496 -  (nodes, edges) <-
    497 -    Text.getContents <&> Parser.parseOnly parser >>= \case
    498 -      Left err -> error $ "failed to parse skyframe graph: " <> err
    499 -      Right graph -> pure graph
    500 -  putStrLn $ "node count = " <> show (length nodes) <> ", edge count = " <> show (length edges)
    501 -  let assignIndex node = gets (,node) <* modify (+ 1)
    502 -      indexedNodes = evalState (for nodes assignIndex) 1
    503 -      coerceMaybe = fromMaybe $ error "parser produced an edge with an unknown node"
    504 -      indexedEdges = coerceMaybe $
    505 -        for (Set.toList edges) $ \(Edge group source target) -> do
    506 -          (sourceIndex, _) <- Map.lookup source indexedNodes
    507 -          (targetIndex, _) <- Map.lookup target indexedNodes
    508 -          pure (group, sourceIndex, targetIndex)
    509 -  Sqlite.batchInsert database "node" ["idx", "hash", "data", "type"] $
    510 -    Map.assocs indexedNodes <&> \(nodeHash, (nodeIdx, Node nodeData nodeType)) ->
    511 -      SQLInteger nodeIdx : (SQLText <$> [nodeHash, nodeType <> ":" <> nodeData, nodeType])
    512 -  Sqlite.batchInsert database "edge" ["group_num", "source", "target"] $
    513 -    indexedEdges <&> \(g, s, t) -> SQLInteger <$> [fromIntegral g, s, t]
-}
  pure ()


repl :: IO ()
repl = do
  --threadDelay 2_000_000
  graph <- parseIt' @(DotGraph Text) . LazyText.pack <$> readFile "/tmp/skyscope.dot"
  let nodes = graphNodes graph
      edges = graphEdges graph
  putStrLn "\x1b[1;37mnodes:\x1b[0m"
  for_ nodes $ \DotNode{..} -> do
    putStrLn $ "  " <> Text.unpack nodeID
    for_ nodeAttributes $ \case
      Label (StrLabel label) -> putStrLn $ "    " <> LazyText.unpack label
      _ -> pure ()
  putStrLn "\x1b[1;37medges:\x1b[0m"
  for_ edges $ \edge -> putStrLn $ show edge
  pure ()

-- $> Import.repl


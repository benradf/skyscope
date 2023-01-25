{-# OPTIONS_GHC -fwarn-unused-imports #-}

{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}

module Main where

import Control.Monad.IO.Class (liftIO)
import qualified Crypto.Hash.SHA256 as SHA256
import Data.Aeson (FromJSON, ToJSON, ToJSONKey)
import qualified Data.Aeson as Json
import Data.Aeson.Types (toJSONKeyText)
import qualified Data.Attoparsec.Combinator as Parser
import Data.Attoparsec.Text (Parser)
import qualified Data.Attoparsec.Text as Parser
import Data.Bifunctor (bimap)
import qualified Data.ByteString as BS
import Data.ByteString.Builder (byteStringHex, toLazyByteString)
import qualified Data.ByteString.Lazy.Char8 as LBSC
import Data.Coerce (coerce)
import Data.FileEmbed (embedFile)
import Data.Functor ((<&>))
import Data.Int (Int64)
import Data.List (nub)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Set (Set)
import qualified Data.Set as Set
import Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Data.Text.IO as Text
import qualified Data.Text.Lazy as LazyText
import Data.Traversable (for)
import Database.SQLite3 (SQLData(..))
import GHC.Generics (Generic)
import Network.HTTP.Types.Status (badRequest400)
import Prelude
import qualified Sqlite
import System.Directory (getCurrentDirectory)
import System.Exit (ExitCode(..))
import System.Posix.Temp (mkdtemp)
import System.Process.Text (readProcessWithExitCode)
import qualified Web.Scotty as Web

data Node = Node
  { nodeType :: Text
  , nodeData :: Text
  } deriving (Eq, Ord, Show, Generic, ToJSON, FromJSON)

newtype NodeHash = NodeHash Text
  deriving (Eq, Ord, Show, Generic, ToJSON, FromJSON)

instance ToJSONKey NodeHash where
  toJSONKey = toJSONKeyText coerce

data Edge = Edge
  { edgeSource :: NodeHash
  , edgeTarget :: NodeHash
  } deriving (Eq, Ord, Show, Generic, ToJSON, FromJSON)

type Graph = (Map NodeHash Node, Set Edge)

main :: IO ()
main = do
  dir <- mkdtemp "skyscope"
  let path = dir <> "/database"
  absolutePath <- getCurrentDirectory <&> (<> ("/" <> path))
  putStrLn $ "\x1b[1;33mdatabase: " <> absolutePath <> "\x1b[0m"
  Sqlite.withDatabase path $ \db ->
    importGraph db *> server db

importGraph :: Sqlite.Database -> IO ()
importGraph db = do
  createSchema db
  (nodes, edges) <- Text.getContents <&> Parser.parseOnly graphParser >>= \case
    Left err -> error $ "failed to parse skyframe graph: " <> err
    Right graph -> pure graph
  Sqlite.batchInsert db "node" [ "hash", "type", "data" ] $
    Map.assocs nodes <&> \(NodeHash hash, Node nodeType nodeData) ->
      SQLText <$> [ hash, nodeType, nodeData ]
  Sqlite.batchInsert db "edge" [ "source", "target" ] $
    Set.toList edges <&> \(Edge (NodeHash source) (NodeHash target)) ->
      SQLText <$> [ source, target ]
  where
    createSchema db = Sqlite.executeStatements db
      [ [ "CREATE TABLE node ("
        , "  hash TEXT PRIMARY KEY,"
        , "  type TEXT,"
        , "  data TEXT"
        , ");"
        ]
      , [ "CREATE TABLE edge ("
        , "  source TEXT,"
        , "  target TEXT,"
        , "  PRIMARY KEY (source, target),"
        , "  FOREIGN KEY (source) REFERENCES node(hash),"
        , "  FOREIGN KEY (target) REFERENCES node(hash)"
        , ");"
        ]
      ]

graphParser :: Parser Graph
graphParser = do
  Parser.option undefined $ Parser.string "Warning:"
  Parser.manyTill Parser.anyChar $ Parser.lookAhead lineParser
  let unions = bimap Map.unions Set.unions . unzip
  graph <- unions <$> lineParser `Parser.sepBy` Parser.endOfLine
  Parser.many' Parser.endOfLine
  Parser.endOfInput
  pure graph
  where
    lineParser :: Parser Graph
    lineParser = do
      nodes@((sourceHash, _) : targets) <- Parser.many1 nodeParser
      pure (Map.fromList nodes, Set.fromList $ Edge sourceHash . fst <$> targets)
    nodeParser :: Parser (NodeHash, Node)
    nodeParser = do
      nodeType <- Parser.takeWhile1 $ \c -> c /= ':' && c /= '\n'
      Parser.char ':'
      nodeData <- Parser.takeTill $ \c -> c == '|' || c == '\n'
      Parser.option undefined $ Parser.char '|'
      let nodeHash = NodeHash $ Text.decodeUtf8 $ LBSC.toStrict $ toLazyByteString $
            byteStringHex $ SHA256.hash $ Text.encodeUtf8 $ nodeType <> ":" <> nodeData
      pure (nodeHash, Node nodeType nodeData)

server :: Sqlite.Database -> IO ()
server db = do
  Web.scotty 28581 $ do
    Web.get "/" $ do
      mainJs <- liftIO $ Text.decodeUtf8 <$> BS.readFile "/home/ben/git/skyscope/src/main.js"
      themeCss <- liftIO $ Text.decodeUtf8 <$> BS.readFile "/home/ben/git/skyscope/src/theme.css"
      Web.html $ LazyText.fromStrict $ Text.unlines
        [ "<html>"
        , "  <head>"
        , "    <title>Skyscope</title>"
        , "    <meta charset=\"UTF-8\">"
        , "    <script>"
        --, Text.decodeUtf8 $(embedFile "src/main.js")
        , mainJs
        , "    </script>"
        , "    <style>"
        , themeCss
        --, Text.decodeUtf8 $(embedFile "src/theme.css")
        , "    </style>"
        , "  </head>"
        , "  <body></body>"
        , "</html>"
        ]
    Web.post "/find" $ Json.eitherDecode <$> Web.body >>= \case
      Right pattern -> Web.json =<< liftIO (findNodes db 100 pattern)
      Left err -> badRequest err
    Web.post "/render" $ Json.eitherDecode <$> Web.body >>= \case
      Right visibleNodes -> do
        Web.setHeader "Content-Type" "image/svg+xml"
        Web.text =<< liftIO (renderSvg db visibleNodes)
      Left err -> badRequest err
  where
    badRequest = Web.raiseStatus badRequest400 . LazyText.pack

findNodes :: Sqlite.Database -> Int64 -> Text -> IO (Int64, Map NodeHash Node)
findNodes db limit pattern = do
  SQLInteger total <- Sqlite.executeSqlScalar db
    [ "SELECT COUNT(hash) FROM node"
    , "WHERE data LIKE ?" ] [ SQLText pattern ]
  records <- Sqlite.executeSql db
    [ "SELECT hash, type, data FROM node"
    , "WHERE data LIKE ? LIMIT ?;" ]
    [ SQLText pattern, SQLInteger limit ]
  pure . (total, ) $ Map.fromList $ records <&> \
    [ SQLText hash, SQLText nodeType, SQLText nodeData ] ->
      (NodeHash hash, Node nodeType nodeData)

renderSvg :: Sqlite.Database -> [NodeHash] -> IO LazyText.Text
renderSvg db hashes = do
  let mapOf = Map.fromList . map (\h -> (h, h))
  nodes <- for (mapOf hashes) $ \(NodeHash hash) -> Sqlite.executeSql db
    [ "SELECT type, data FROM node WHERE hash = ?;" ] [ SQLText hash ] <&> \
    [ [ SQLText nodeType, SQLText nodeData ] ] -> Node nodeType nodeData
  edges <- fmap (nub . concat) $ for hashes $ \(NodeHash hash) -> Sqlite.executeSql db
    [ "SELECT source, target FROM edge WHERE source = ?1 OR target = ?1;" ] [ SQLText hash ] <&>
    map (\[ SQLText source, SQLText target ] -> Edge (NodeHash source) (NodeHash target))
  let graph = Text.concat
        [ "digraph {\n"
        , Text.unlines $ Map.assocs nodes <&> \(NodeHash hash, Node nodeType nodeData) ->
            --"node_" <> hash <> " [label=\"" <> nodeType <> ":" <> nodeData <> "\"];"
            "    node_" <> hash <> " [label=\"" <> nodeType <> "\"];"
        , Text.unlines $ edges <&> \(Edge (NodeHash source) (NodeHash target)) ->
            "    node_" <> source <> " -> node_" <> target <> ";"
        , "\n}"
        ]
  Text.writeFile "/tmp/skyscope.dot" graph
  readProcessWithExitCode "dot" [ "-Tsvg" ] graph <&> \case
    (ExitFailure code, _, err) -> error $ "dot exit " <> show code <> ": " <> Text.unpack err
    (ExitSuccess, svg, _) -> LazyText.fromStrict svg

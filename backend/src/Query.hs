{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}

module Query where

import Common
import Control.Category ((>>>))
import Control.Concurrent (forkIO, getNumCapabilities, threadDelay)
import Control.Concurrent.MVar (MVar, newEmptyMVar, newMVar, putMVar, readMVar)
import Control.Concurrent.STM (atomically, retry)
import Control.Concurrent.STM.TVar (TVar, modifyTVar, newTVarIO, readTVar, readTVarIO, stateTVar, writeTVar)
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.RWS (RWS, evalRWS)
import Control.Monad.RWS.Class
import Data.Aeson (FromJSON, ToJSON)
import Data.Bifunctor (first)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Foldable (asum, for_)
import Data.Functor (void, (<&>))
import Data.HList (Label (..))
import Data.HList.Record (HasField, hLookupByLabel)
import Data.Int (Int64)
import Data.IntMap (IntMap)
import qualified Data.IntMap as IntMap
import Data.List (nub, sort, sortOn, uncons)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe (catMaybes, fromMaybe, isJust)
import Data.Set (Set)
import Data.Text (Text)
import Data.Traversable (for)
import Database.SQLite3 (SQLData (..))
import Foreign.C.Types (CInt (..), CLong (..))
import qualified Foreign.Marshal.Array as Marshal
import Foreign.Ptr (Ptr, castPtr)
import Foreign.Storable (sizeOf)
import GHC.Generics (Generic)
import Model
import Sqlite (Database)
import qualified Sqlite
import Prelude

type Pattern = Text

data QueryResult = QueryResult
  { resultTotalNodes :: Int,
    resultNodes :: NodeMap Node
  }
  deriving (Eq, Ord, Show, Generic, ToJSON, FromJSON)

type FindPathMemo = TVar (Map (NodeHash, NodeHash) [NodeHash])

type HasFindPathMemo r = (HasField "findPath" r FindPathMemo, HasMakePathFinderMemo r)

getFindPathMemo :: HasFindPathMemo r => r -> FindPathMemo
getFindPathMemo = hLookupByLabel (Label :: Label "findPath")

findPath :: HasFindPathMemo r => Database -> NodeHash -> NodeHash -> Memoize r [NodeHash]
findPath database = curry $
  memoize "findPath" getFindPathMemo $ \(origin, destination) ->
    liftIO . readMVar =<< findPathAsync database origin destination

findPathAsync :: HasMakePathFinderMemo r => Database -> NodeHash -> NodeHash -> Memoize r (MVar [NodeHash])
findPathAsync database origin destination = do
  pathFinder <- makePathFinder database ()
  liftIO $ pathFinder origin destination

type PathFinder = NodeHash -> NodeHash -> IO (MVar [NodeHash])

type MakePathFinderMemo = TVar (Map () PathFinder)

type HasMakePathFinderMemo r = HasField "makePathFinder" r MakePathFinderMemo

getMakePathFinderMemo :: HasMakePathFinderMemo r => r -> MakePathFinderMemo
getMakePathFinderMemo = hLookupByLabel (Label :: Label "makePathFinder")

makePathFinder :: HasMakePathFinderMemo r => Database -> () -> Memoize r PathFinder
makePathFinder database = memoize "makePathFinder" getMakePathFinderMemo $ \() -> liftIO $ do
  void $ Sqlite.executeSql database ["DELETE FROM path;"] []
  nodeCount <- Sqlite.executeSqlScalar database ["SELECT COUNT(idx) FROM node;"] [] <&> fromSQLInt
  predMap <- makePredMap nodeCount
  let predMapSize = length predMap
  --predMapPtr <- Marshal.mallocArray predMapSize
  --Marshal.pokeArray predMapPtr $ fromIntegral <$> predMap
  --stepMapPtr <- Marshal.mallocArray nodeCount

  let findPath :: NodeHash -> NodeHash -> IO [NodeHash]
      findPath origin destination = do
        origin <- getNodeIdx origin
        destination <- getNodeIdx destination
        steps <-
          Sqlite.executeSql
            database
            ["SELECT steps FROM path WHERE destination = ?;"]
            [SQLInteger destination]
            >>= \case
              [[SQLBlob steps]] -> pure steps
              [] -> undefined -- TODO: compute
              [_] -> error "steps column has unexpected data type"
              _ : _ : _ -> error "should be impossible due to primary key constraint on destination column"
        let stepMapBytes = BS.unpack steps
            stepMapSizeBytes = length stepMapBytes
            stepMapSize = stepMapSizeBytes `div` sizeOf (0 :: CLong)
        if stepMapSize * sizeOf (0 :: CLong) /= stepMapSizeBytes
          then error $ "misaligned path data for " <> show destination
          else Marshal.allocaArray stepMapSize $ \stepMapPtr -> do
            Marshal.pokeArray (castPtr stepMapPtr) stepMapBytes
            let maxLength = 1048576 -- 4MiB of int32_t
            Marshal.allocaArray maxLength $ \pathPtr -> do
              actualLength <-
                fromIntegral
                  <$> Query.c_findPath
                    (fromIntegral origin)
                    (fromIntegral destination)
                    stepMapPtr
                    (fromIntegral stepMapSize)
                    pathPtr
                    (fromIntegral maxLength)
              if actualLength == -1
                then error "exceeded max path length"
                else do
                  path <- Marshal.peekArray actualLength pathPtr
                  for (fromIntegral <$> path) $ \nodeIdx ->
                    Sqlite.executeSql
                      database
                      ["SELECT hash FROM node WHERE idx = ?;"]
                      [SQLInteger nodeIdx]
                      <&> \case
                        [] -> error $ "failed to find hash for path node " <> show nodeIdx
                        [[SQLText nodeHash]] -> nodeHash
                        _ : _ : _ -> error "should be impossible due to primary key constraint on idx column"
                        [_] -> error "hash column has unexpected data type"

  pure $ \origin destination -> do
    result <- newEmptyMVar
    forkIO $ putMVar result =<< findPath origin destination
    pure result
  where
    {-
        getStepsTo :: Int64 -> IO ByteString
        getStepsTo idx = do
          Sqlite.executeSql database ["SELECT steps FROM path WHERE destination = ?;"] [SQLInteger idx] >>= \case
            [[SQLBlob steps]] -> pure steps
            [] -> undefined -- TODO: compute
    -}

    {-
      destinations <- newTVarIO [1 .. fromIntegral nodeCount]
      let nextDest =
            atomically $
              stateTVar destinations $
                uncons >>> \case
                  Just (next, remaining) -> (Just next, remaining)
                  Nothing -> (Nothing, [])
          worker stepMapPtr =
            nextDest >>= \case
              Just destination -> do
                stepMapSize <- Query.c_indexPaths predMapPtr destination (fromIntegral nodeCount) stepMapPtr
                let stepMapSizeBytes = fromIntegral stepMapSize * sizeOf (0 :: CLong)
                stepMapBytes <- Marshal.peekArray stepMapSizeBytes $ castPtr stepMapPtr
                void $
                  Sqlite.executeSql
                    database
                    ["INSERT INTO path (destination, steps) VALUES (?, ?);"]
                    [SQLInteger $ fromIntegral destination, SQLBlob $ BS.pack stepMapBytes]
                --atomically $ modifyTVar progress $ first (+ 1)
                worker stepMapPtr
              Nothing -> pure ()
      workerCount <- getNumCapabilities <&> (subtract 1)
      for_ [1 .. max 1 workerCount] $ const $ forkIO $ Marshal.allocaArray nodeCount worker
      let wait = atomically $ do
            remaining <- length <$> readTVar destinations
            if remaining > 0 then retry else pure ()
      wait

      pure $ \_ _ -> do
        result <- newMVar []
        pure result
    -}

    getNodeIdx :: NodeHash -> IO Int64
    getNodeIdx hash =
      Sqlite.executeSqlScalar
        database
        ["SELECT idx FROM node WHERE hash = ?;"]
        [SQLText hash]
        <&> \(SQLInteger n) -> n

    makePredMap :: Int -> IO [Int]
    makePredMap nodeCount = do
      predecessors <-
        Sqlite.executeSql database ["SELECT target, source FROM edge ORDER BY target;"] []
          <&> ((map $ \[t, s] -> (fromSQLInt t, [fromSQLInt s])) >>> IntMap.fromAscListWith (++))
      pure $ uncurry (++) $ evalRWS (for [0 .. nodeCount] serialisePreds) predecessors 0

    serialisePreds :: Int -> RWS (IntMap [Int]) [Int] Int Int
    serialisePreds i =
      asks (IntMap.lookup i) <&> fromMaybe [] >>= \case
        [pred] -> pure pred
        preds -> do
          let n = length preds
          tell $ n : preds
          offset <- get <* modify (+ (1 + n))
          pure $ negate offset

    fromSQLInt :: Num a => SQLData -> a
    fromSQLInt (SQLInteger n) = fromIntegral n
    fromSQLInt value = error $ "expected data type" <> show value

foreign import ccall safe "path.cpp"
  c_indexPaths ::
    Ptr CInt -> -- predMap
    CInt -> -- destination
    CInt -> -- nodeCount
    Ptr CLong -> -- stepMap
    IO CInt

--  result <- liftIO newEmptyMVar
--  let worker = do
--        origin <- getNodeIdx origin
--        destination <- getNodeIdx destination
--        steps <- getStepsTo destination
--        undefined
--  liftIO $ forkIO $ worker
--  pure result

{-
  where
    getNodeIdx :: NodeHash -> IO Int64
    getNodeIdx hash =
      Sqlite.executeSqlScalar
        database
        ["SELECT idx FROM node WHERE hash = ?;"]
        [SQLText hash]
        <&> \(SQLInteger n) -> n
    getStepsTo :: Int64 -> IO ByteString
    getStepsTo idx = do
      Sqlite.executeSql database ["SELECT steps FROM path WHERE destination = ?;"] [SQLInteger idx] >>= \case
        [[SQLBlob steps]] -> pure steps
        [] -> undefined -- TODO: compute
-}

foreign import ccall safe "path.cpp"
  c_findPath ::
    CInt -> -- origin
    CInt -> -- destination
    Ptr CLong -> -- stepMap
    CInt -> -- stepMapSize
    Ptr CInt -> -- pathBuffer
    CInt -> -- maxLength
    IO CInt

type FloodNodesMemo = TVar (Map (NodeHash, Pattern, Set NodeType) QueryResult)

type HasFloodNodesMemo r = HasField "floodNodes" r FloodNodesMemo

getFloodNodesMemo :: HasFloodNodesMemo r => r -> FloodNodesMemo
getFloodNodesMemo = hLookupByLabel (Label :: Label "floodNodes")

floodNodes ::
  HasFloodNodesMemo r =>
  Database ->
  Int ->
  NodeHash ->
  Pattern ->
  Set NodeType ->
  Memoize r QueryResult
floodNodes _database _limit _source _pattern _types = error "not implemented"

type FilterNodesMemo = TVar (Map Pattern QueryResult)

type HasFilterNodesMemo r = HasField "filterNodes" r FilterNodesMemo

getFilterNodesMemo :: HasFilterNodesMemo r => r -> FilterNodesMemo
getFilterNodesMemo = hLookupByLabel (Label :: Label "filterNodes")

filterNodes ::
  HasFilterNodesMemo r =>
  Database ->
  Int64 ->
  Pattern ->
  Memoize r QueryResult
filterNodes database limit = memoize "filterNodes" getFilterNodesMemo $ \pattern -> do
  SQLInteger total <-
    liftIO $
      Sqlite.executeSqlScalar
        database
        ["SELECT COUNT(hash) FROM node WHERE data LIKE ?"]
        [SQLText pattern]
  records <-
    liftIO $
      Sqlite.executeSql
        database
        ["SELECT hash, data, type FROM node WHERE data LIKE ? LIMIT ?;"]
        [SQLText pattern, SQLInteger limit]
  pure . QueryResult (fromIntegral total) $
    Map.fromList $
      records <&> \[SQLText hash, SQLText nodeData, SQLText nodeType] ->
        (hash, Node nodeData nodeType)

type GetNeighboursMemo = TVar (Map NodeHash [NodeHash])

type HasGetNeighboursMemo r = HasField "getNeighbours" r GetNeighboursMemo

getGetNeighboursMemo :: HasGetNeighboursMemo r => r -> GetNeighboursMemo
getGetNeighboursMemo = hLookupByLabel (Label :: Label "getNeighbours")

getNeighbours ::
  HasGetNeighboursMemo r =>
  Database ->
  NodeHash ->
  Memoize r [NodeHash]
getNeighbours database = memoize "getNeighbours" getGetNeighboursMemo $ \nodeHash -> do
  incomingEdges <- liftIO $ selectEdges database nodeHash "t.hash = ?"
  outgoingEdges <- liftIO $ selectEdges database nodeHash "s.hash = ?"
  pure $ filter (/= nodeHash) $ sort $ nub $ concat $ projectEdge <$> (incomingEdges <> outgoingEdges)
  where
    projectEdge :: [SQLData] -> [NodeHash]
    projectEdge [_, SQLText source, SQLText target] = [source, target]
    projectEdge _ = error "sql pattern match unexpectedly failed"

type GetContextMemo = TVar (Map [Text] (Map Text Text))

type HasGetContextMemo r = HasField "getContext" r GetContextMemo

getContextMemo :: HasGetContextMemo r => r -> GetContextMemo
getContextMemo = hLookupByLabel (Label :: Label "getContext")

getContext ::
  HasGetContextMemo r => Database -> [Text] -> Memoize r (Map Text Text)
getContext database = memoize "getContext" getContextMemo $
  fmap (fmap (Map.fromList . catMaybes)) $
    traverse $ \key ->
      liftIO $
        Sqlite.executeSql
          database
          ["SELECT context_data FROM context WHERE context_key LIKE ?;"]
          [SQLText key]
          <&> \case
            [[SQLText contextData]] -> Just (key, contextData)
            _ -> Nothing

selectEdges :: Database -> NodeHash -> Text -> IO [[SQLData]]
selectEdges database nodeHash whereClause =
  Sqlite.executeSql
    database
    [ "SELECT group_num, s.hash, t.hash FROM edge",
      "INNER JOIN node AS s ON s.idx = edge.source",
      "INNER JOIN node AS t ON t.idx = edge.target",
      "WHERE " <> whereClause <> ";"
    ]
    [SQLText nodeHash]

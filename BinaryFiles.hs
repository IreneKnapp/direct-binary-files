{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, TypeSynonymInstances,
             FlexibleInstances, StandaloneDeriving, ExistentialQuantification,
             FlexibleContexts, FunctionalDependencies, Rank2Types,
             DeriveDataTypeable, ScopedTypeVariables #-}
module BinaryFiles
  (Endianness(..),
   HasEndianness(..),
   Serializable,
   serialize,
   deserialize,
   Serialization,
   Deserialization,
   ContextualSerialization,
   ContextualDeserialization,
   SomeSerializationFailure(..),
   SerializationFailure(..),
   LowLevelSerializationFailure(..),
   OutOfRangeSerializationFailure(..),
   InsufficientDataSerializationFailure(..),
   seek,
   tell,
   isEOF,
   SerialOrigin(..),
   read,
   write,
   throw,
   catch,
   getContext,
   withContext,
   getTags,
   withTag,
   withWindow,
   runSerializationToByteString,
   runSerializationToFile,
   runDeserializationFromByteString,
   runDeserializationFromFile,
   runSubDeserializationFromByteString,
   toByteString,
   toFile,
   fromByteString,
   fromFile,
   serializeWord,
   deserializeWord,
   serializeNullTerminatedText,
   deserializeNullTerminatedText,
   serializeNullPaddedText,
   deserializeNullPaddedText)
  where

import Control.Exception (Exception, IOException)
import qualified Control.Exception as E
import Data.Bits
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.List
import Data.Typeable
import Numeric
import Prelude hiding (read, catch)
import qualified Prelude as P
import System.IO hiding (isEOF)


data Endianness = BigEndian | LittleEndian
                deriving (Eq, Show)


class HasEndianness hasEndianness where
  considerEndianness :: hasEndianness -> Endianness
instance HasEndianness Endianness where
  considerEndianness = id


type Serialization a =
  forall context
  . ContextualSerialization context a


type Deserialization a =
  forall context
  . ContextualDeserialization context a


newtype ContextualSerialization context a =
  ContextualSerialization {
      contextualSerializationAction
        :: forall backend
        .  (BackendSpecificMonadSerial BackendSpecificSerialization backend,
            MonadSerial (BackendSpecificSerialization backend),
            MonadSerialWriter BackendSpecificSerialization backend)
        => BackendSpecificSerialization backend context a
    }


newtype ContextualDeserialization context a =
  ContextualDeserialization {
      contextualDeserializationAction
        :: forall backend
        .  (BackendSpecificMonadSerial BackendSpecificDeserialization backend,
            MonadSerial (BackendSpecificDeserialization backend),
            MonadSerialReader BackendSpecificDeserialization backend)
        => BackendSpecificDeserialization backend context a
    }


data SerialOrigin
  = OffsetFromStart
  | OffsetFromCurrent
  | OffsetFromEnd
  deriving (Eq, Ord, Show)


data Window underlying =
  Window {
      windowStart :: Int,
      windowLength :: Int
    }


data Identity a =
  Identity {
      identityAction :: a
    }


data BackendSpecificSerialization backend context a =
  BackendSpecificSerialization {
      serializationAction
        :: Internals BackendSpecificSerialization backend
        -> context
        -> [(Int, String)]
        -> PrimitiveMonad backend
             (Either (Int, [(Int, String)], SomeSerializationFailure)
                     (Internals BackendSpecificSerialization backend, a))
    }


data BackendSpecificDeserialization backend context a =
  BackendSpecificDeserialization {
      deserializationAction
        :: Internals BackendSpecificDeserialization backend
        -> context
        -> [(Int, String)]
        -> PrimitiveMonad backend
             (Either (Int, [(Int, String)], SomeSerializationFailure)
                     (Internals BackendSpecificDeserialization backend, a))
    }


data SomeSerializationFailure =
  forall failure . SerializationFailure failure
  => SomeSerializationFailure failure
     deriving (Typeable)


data LowLevelSerializationFailure =
  LowLevelSerializationFailure IOException
  deriving (Typeable)


data OutOfRangeSerializationFailure =
  OutOfRangeSerializationFailure Int
  deriving (Typeable)


data InsufficientDataSerializationFailure =
  InsufficientDataSerializationFailure Int
  deriving (Typeable)


class Serial backend where
  data SerialDataSource backend
  type PrimitiveMonad backend :: * -> *
  type Underlying backend
  backend
    :: SerialDataSource backend
    -> backend


class Serializable context a where
  serialize :: a -> ContextualSerialization context ()
  deserialize :: ContextualDeserialization context a


class (Show failure, Typeable failure) => SerializationFailure failure where
  toSerializationFailure :: failure -> SomeSerializationFailure
  fromSerializationFailure :: SomeSerializationFailure -> Maybe failure
  
  toSerializationFailure failure =
    SomeSerializationFailure failure
  fromSerializationFailure someFailure = case someFailure of
    SomeSerializationFailure failure -> cast failure


class (Serial backend,
       Monad (PrimitiveMonad backend))
      => BackendSpecificMonadSerial m backend
      where  
  data Internals m backend
  getInternals
    :: m backend context (Internals m backend)
  putInternals
    :: Internals m backend
    -> m backend context ()
  internalsDataSource
    :: Internals m backend
    -> SerialDataSource backend


-- IAK
class MonadSerial m where
  throw
    :: forall context failure a
    .  (Monad (m context),
        SerializationFailure failure)
    => failure
    -> m context a
  catch
    :: forall context failure a
    . (Monad (m context),
       SerializationFailure failure)
    => m context a
    -> (Int -> [(Int, String)] -> failure -> m context a)
    -> m context a
  seek
    :: SerialOrigin -> Int -> m context ()
  tell
    :: m context Int
  primitiveTell
    :: m context Int
  isEOF
    :: m context Bool


class BackendSpecificMonadSerial m backend
      => MonadSerialWindow m backend where
  windowInternals
    :: SerialDataSource backend
    -> Internals m (Underlying backend)
    -> Internals m backend
  windowInternalsUnderlying
    :: Internals m backend
    -> Internals m (Underlying backend)
  updateWindowInternalsUnderlying
    :: Internals m backend
    -> Internals m (Underlying backend)
    -> Internals m backend
  
  runInUnderlying
    :: BackendSpecificMonadSerial m (Underlying backend)
    => m (Underlying backend) context a
    -> m backend context a


class (BackendSpecificMonadSerial m backend)
      => MonadSerialReader m backend
      where
  read :: Int -> m backend context ByteString


class (BackendSpecificMonadSerial m backend)
      => MonadSerialWriter m backend
      where
  write :: ByteString -> m backend context ()


class BackendSpecificMonadSerial m ByteString
      => MonadSerialByteString m
      where
  byteStringInternalsOffset
    :: Internals m ByteString
    -> Int
  
  updateByteStringInternalsOffset
    :: Int
    -> Internals m ByteString
    -> Internals m ByteString


class BackendSpecificMonadSerial m backend
      => MonadSerialIO m backend
      where
  catchIO
    :: Exception e
    => IO a
    -> (e -> m backend context a)
    -> m backend context a


instance Serial ByteString where
  data SerialDataSource ByteString =
    ByteStringSerialDataSource {
        byteStringSerialDataSourceByteString :: ByteString
      }
  type PrimitiveMonad ByteString = Identity
  type Underlying ByteString = ByteString
  backend = byteStringSerialDataSourceByteString


instance Serial FilePath where
  data SerialDataSource FilePath =
    FilePathSerialDataSource {
        filePathSerialDataSourceFilePath :: FilePath
      }
  type PrimitiveMonad FilePath = IO
  type Underlying FilePath = FilePath
  backend = filePathSerialDataSourceFilePath


instance Serial underlying => Serial (Window underlying) where
  data SerialDataSource (Window underlying) =
    WindowDataSource {
        windowDataSourceUnderlying :: SerialDataSource underlying,
        windowDataSourceWindow :: Window underlying
      }
  type PrimitiveMonad (Window underlying) = PrimitiveMonad underlying
  type Underlying (Window underlying) = underlying
  backend = windowDataSourceWindow


instance Monad Identity where
  return a = Identity a
  (Identity x) >>= f = f x


instance Monad (PrimitiveMonad backend)
         => Monad (BackendSpecificSerialization backend context) where
  return a = BackendSpecificSerialization $ \internals _ _ ->
               return $ Right (internals, a)
  (BackendSpecificSerialization x) >>= f =
    BackendSpecificSerialization $ \internals context tags -> do
      v <- x internals context tags
      case v of
        Left failure -> return $ Left failure
        Right (internals', y) ->
          serializationAction (f y) internals' context tags


instance Monad (PrimitiveMonad backend)
         => Monad (BackendSpecificDeserialization backend context) where
  return a = BackendSpecificDeserialization $ \internals _ _ ->
    return $ Right (internals, a)
  (BackendSpecificDeserialization x) >>= f =
    BackendSpecificDeserialization $ \internals context tags -> do
      v <- x internals context tags
      case v of
        Left failure -> return $ Left failure
        Right (internals', y) ->
          deserializationAction (f y) internals' context tags


instance BackendSpecificMonadSerial BackendSpecificSerialization ByteString
         where
  data Internals BackendSpecificSerialization ByteString =
    ByteStringSerializationInternals {
        byteStringSerializationInternalsDataSource
          :: SerialDataSource ByteString,
        byteStringSerializationInternalsOffset
          :: Int
      }
  getInternals =
    BackendSpecificSerialization $ \internals _ _ ->
      return $ Right (internals, internals)
  putInternals internals =
    BackendSpecificSerialization $ \_ _ _ ->
      return $ Right (internals, ())
  internalsDataSource = byteStringSerializationInternalsDataSource


instance BackendSpecificMonadSerial BackendSpecificSerialization FilePath
         where
  data Internals BackendSpecificSerialization FilePath = 
    FilePathSerializationInternals {
        filePathSerializationInternalsDataSource
          :: SerialDataSource FilePath,
        filePathSerializationInternalsHandle
          :: Handle
      }
  getInternals =
    BackendSpecificSerialization $ \internals _ _ ->
      return $ Right (internals, internals)
  putInternals internals =
    BackendSpecificSerialization $ \_ _ _ ->
      return $ Right (internals, ())
  internalsDataSource = filePathSerializationInternalsDataSource


instance (BackendSpecificMonadSerial BackendSpecificSerialization
                                     underlying)
         => BackendSpecificMonadSerial BackendSpecificSerialization
                                       (Window underlying)
         where
  data Internals BackendSpecificSerialization (Window underlying) =
    WindowSerializationInternals {
        windowSerializationInternalsDataSource
          :: SerialDataSource (Window underlying),
        windowSerializationInternalsUnderlying
          :: Internals BackendSpecificSerialization underlying
      }
  getInternals =
    BackendSpecificSerialization $ \internals _ _ ->
      return $ Right (internals, internals)
  putInternals internals =
    BackendSpecificSerialization $ \_ _ _ ->
      return $ Right (internals, ())
  internalsDataSource = windowSerializationInternalsDataSource


instance BackendSpecificMonadSerial BackendSpecificDeserialization ByteString
         where
  data Internals BackendSpecificDeserialization ByteString = 
    ByteStringDeserializationInternals {
        byteStringDeserializationInternalsDataSource
          :: SerialDataSource ByteString,
        byteStringDeserializationInternalsOffset
          :: Int
      }
  getInternals =
    BackendSpecificDeserialization $ \internals _ _ ->
      return $ Right (internals, internals)
  putInternals internals =
    BackendSpecificDeserialization $ \_ _ _ ->
      return $ Right (internals, ())
  internalsDataSource = byteStringDeserializationInternalsDataSource


instance BackendSpecificMonadSerial BackendSpecificDeserialization FilePath
         where
  data Internals BackendSpecificDeserialization FilePath = 
    FilePathDeserializationInternals {
        filePathDeserializationInternalsDataSource
          :: SerialDataSource FilePath,
        filePathDeserializationInternalsHandle
          :: Handle
      }
  getInternals =
    BackendSpecificDeserialization $ \internals _ _ ->
      return $ Right (internals, internals)
  putInternals internals =
    BackendSpecificDeserialization $ \_ _ _ ->
      return $ Right (internals, ())
  internalsDataSource = filePathDeserializationInternalsDataSource


instance (BackendSpecificMonadSerial BackendSpecificDeserialization
                                     underlying)
         => BackendSpecificMonadSerial BackendSpecificDeserialization
                                       (Window underlying)
         where
  data Internals BackendSpecificDeserialization (Window underlying) =
    WindowDeserializationInternals {
        windowDeserializationInternalsDataSource
          :: SerialDataSource (Window underlying),
        windowDeserializationInternalsUnderlying
          :: Internals BackendSpecificDeserialization underlying
      }
  getInternals =
    BackendSpecificDeserialization $ \internals _ _ ->
      return $ Right (internals, internals)
  putInternals internals =
    BackendSpecificDeserialization $ \_ _ _ ->
      return $ Right (internals, ())
  internalsDataSource = windowDeserializationInternalsDataSource


instance MonadSerial (BackendSpecificSerialization ByteString) where  
  throw failure = throwImplementation BackendSpecificSerialization failure
  catch action handler =
    catchImplementation BackendSpecificSerialization serializationAction
                        action handler
  seek = byteStringSeek
  tell = byteStringTell
  primitiveTell = tell
  isEOF = byteStringIsEOF


instance MonadSerial (BackendSpecificSerialization FilePath) where
  throw failure = throwImplementation BackendSpecificSerialization failure
  catch action handler =
    catchImplementation BackendSpecificSerialization serializationAction
                        action handler
  seek origin offset = do
    internals <- getInternals
    handleSeek (filePathSerializationInternalsHandle internals)
               origin
               offset
  tell = do
    internals <- getInternals
    handleTell $ filePathSerializationInternalsHandle internals
  primitiveTell = tell
  isEOF = do
    internals <- getInternals
    handleIsEOF $ filePathSerializationInternalsHandle internals


instance (BackendSpecificMonadSerial BackendSpecificSerialization underlying,
          MonadSerial (BackendSpecificSerialization underlying))
         => MonadSerial (BackendSpecificSerialization (Window underlying))
         where
  throw failure = throwImplementation BackendSpecificSerialization failure
  catch action handler =
    catchImplementation BackendSpecificSerialization serializationAction
                        action handler
  seek = windowSeek
  tell = windowTell
  primitiveTell = runInUnderlying primitiveTell
  isEOF = windowIsEOF


instance MonadSerial (BackendSpecificDeserialization ByteString)
         where
  throw failure = throwImplementation BackendSpecificDeserialization failure
  catch action handler =
    catchImplementation BackendSpecificDeserialization deserializationAction
                        action handler
  seek = byteStringSeek
  tell = byteStringTell
  primitiveTell = tell
  isEOF = byteStringIsEOF


instance MonadSerial (BackendSpecificDeserialization FilePath) where
  throw failure = throwImplementation BackendSpecificDeserialization failure
  catch action handler =
    catchImplementation BackendSpecificDeserialization deserializationAction
                        action handler
  seek origin offset = do
    internals <- getInternals
    handleSeek (filePathDeserializationInternalsHandle internals)
               origin
               offset
  tell = do
    internals <- getInternals
    handleTell $ filePathDeserializationInternalsHandle internals
  primitiveTell = tell
  isEOF = do
    internals <- getInternals
    handleIsEOF $ filePathDeserializationInternalsHandle internals


instance (BackendSpecificMonadSerial BackendSpecificDeserialization underlying,
          MonadSerial (BackendSpecificDeserialization underlying))
         => MonadSerial (BackendSpecificDeserialization (Window underlying))
         where
  throw failure = throwImplementation BackendSpecificDeserialization failure
  catch action handler =
    catchImplementation BackendSpecificDeserialization deserializationAction
                        action handler
  seek = windowSeek
  tell = windowTell
  primitiveTell = runInUnderlying primitiveTell
  isEOF = windowIsEOF


instance MonadSerial ContextualSerialization where
  throw failure = ContextualSerialization $ throw failure
  catch action handler =
    ContextualSerialization
     $ catch (contextualSerializationAction action)
             (\offset tags failure ->
                contextualSerializationAction $ handler offset tags failure)
  seek origin offset = ContextualSerialization $ seek origin offset
  tell = ContextualSerialization tell
  primitiveTell = ContextualSerialization primitiveTell
  isEOF = ContextualSerialization isEOF


instance MonadSerial ContextualDeserialization where
  throw failure = ContextualDeserialization $ throw failure
  catch action handler =
    ContextualDeserialization
     $ catch (contextualDeserializationAction action)
             (\offset tags failure ->
                contextualDeserializationAction $ handler offset tags failure)
  seek origin offset = ContextualDeserialization $ seek origin offset
  tell = ContextualDeserialization tell
  primitiveTell = ContextualDeserialization primitiveTell
  isEOF = ContextualDeserialization isEOF


instance BackendSpecificMonadSerial BackendSpecificSerialization underlying
         => MonadSerialWindow BackendSpecificSerialization
                              (Window underlying) where
  windowInternals =
    WindowSerializationInternals
  windowInternalsUnderlying internals =
    windowSerializationInternalsUnderlying internals
  updateWindowInternalsUnderlying internals underlying =
    internals { windowSerializationInternalsUnderlying = underlying }
  
  runInUnderlying = windowRunInUnderlying
  


instance BackendSpecificMonadSerial BackendSpecificDeserialization underlying
         => MonadSerialWindow BackendSpecificDeserialization
                              (Window underlying) where
  windowInternals =
    WindowDeserializationInternals
  windowInternalsUnderlying internals =
    windowDeserializationInternalsUnderlying internals
  updateWindowInternalsUnderlying internals underlying =
    internals { windowDeserializationInternalsUnderlying = underlying }
  
  runInUnderlying = windowRunInUnderlying


instance MonadSerialWriter BackendSpecificSerialization
                           ByteString
                           where
  write = byteStringWrite


instance MonadSerialWriter BackendSpecificSerialization
                           FilePath
                           where
  write byteString = do
    internals <- getInternals
    handleWrite (filePathSerializationInternalsHandle internals)
                byteString


instance (MonadSerialWriter BackendSpecificSerialization
                            underlying)
          => MonadSerialWriter BackendSpecificSerialization
                               (Window underlying)
                               where
  write = windowWrite


instance MonadSerialReader BackendSpecificDeserialization
                           ByteString
                           where
  read = byteStringRead


instance MonadSerialReader BackendSpecificDeserialization
                           FilePath
                           where
  read nBytes = do
    internals <- getInternals
    handleRead (filePathDeserializationInternalsHandle internals)
               nBytes


instance (MonadSerial (BackendSpecificDeserialization underlying),
          MonadSerialReader BackendSpecificDeserialization underlying)
          => MonadSerialReader BackendSpecificDeserialization
                               (Window underlying)
                               where
  read = windowRead


instance MonadSerialByteString BackendSpecificSerialization
                               where
  byteStringInternalsOffset = byteStringSerializationInternalsOffset
  
  updateByteStringInternalsOffset newOffset internals =
    internals {
        byteStringSerializationInternalsOffset = newOffset
      }


instance MonadSerialByteString BackendSpecificDeserialization where
  byteStringInternalsOffset = byteStringDeserializationInternalsOffset
  
  updateByteStringInternalsOffset newOffset internals =
      internals {
          byteStringDeserializationInternalsOffset = newOffset
        }


instance MonadSerialIO BackendSpecificSerialization
                       FilePath
                       where
  catchIO action handler =
    BackendSpecificSerialization $ \internals context tags -> do
      E.catch (do
                 result <- action
                 return $ Right (internals, result))
              (\exception ->
                 serializationAction (handler exception)
                                     internals context tags)


instance MonadSerialIO BackendSpecificDeserialization
                       FilePath
                       where
  catchIO action handler =
    BackendSpecificDeserialization $ \internals context tags -> do
      E.catch (do
                 result <- action
                 return $ Right (internals, result))
              (\exception ->
                 deserializationAction (handler exception)
                                       internals context tags)


instance SerializationFailure SomeSerializationFailure where
  toSerializationFailure = id
  fromSerializationFailure = Just


instance SerializationFailure LowLevelSerializationFailure


instance SerializationFailure OutOfRangeSerializationFailure


instance SerializationFailure InsufficientDataSerializationFailure


instance Show SomeSerializationFailure where
  show (SomeSerializationFailure e) = show e


instance Show LowLevelSerializationFailure where
  show (LowLevelSerializationFailure e) =
    "Low-level serialization failure: " ++ show e


instance Show OutOfRangeSerializationFailure where
  show (OutOfRangeSerializationFailure offset) =
    "Out-of-range at " ++ show offset


instance Show InsufficientDataSerializationFailure where
  show (InsufficientDataSerializationFailure readLength) =
    "Insufficient data for read of " ++ show readLength ++ " bytes"


throwImplementation constructor failure = do
  offset <- primitiveTell
  constructor $ \_ _ tags ->
    return $ Left (offset, tags, toSerializationFailure failure)


catchImplementation constructor accessor action handler = do
  initialOffset <- tell
  constructor $ \internals context tags -> do
    result <- accessor action internals context tags
    case result of
      Left (failureOffset, failureTags, failure) ->
        case fromSerializationFailure failure of
          Nothing -> return result
          Just specificFailure ->
            accessor
             (do
               seek OffsetFromStart initialOffset
               handler failureOffset failureTags specificFailure)
             internals context tags
      Right _ -> return result


-- IAK
getContext
  :: (Monad (m backend context),
      BackendSpecificMonadSerial m backend)
  => m backend context context
getContext = undefined -- TODO
{-
getContext =
  monadSerialConstructor $ \internals context tags ->
    return $ Right (internals, context)


withContext
  :: BackendSpecificMonadSerial m backend
  => context'
  -> m backend context' a
  -> m backend context a
withContext context x =
  monadSerialConstructor $ \internals _ tags -> do
    v <- monadSerialActionAccessor x internals context tags
    case v of
      Left failure -> return $ Left failure
      Right (internals', y) -> return $ Right (internals', y)


getTags
  :: (Monad (m backend context),
      BackendSpecificMonadSerial m backend)
  => m backend context [(Int, String)]
getTags =
  monadSerialConstructor $ \internals context tags ->
    return $ Right (internals, tags)


withTag
  :: (Monad (m backend context),
      BackendSpecificMonadSerial m backend,
      MonadSerial (m backend))
  => String
  -> m backend context a
  -> m backend context a
withTag tagText action = do
  tagOffset <- primitiveTell
  tags <- getTags
  let tags' = (tagOffset, tagText) : tags
  monadSerialConstructor $ \internals context _ ->
    monadSerialActionAccessor action internals context tags'


withWindow
  :: (Monad (m backend context),
      BackendSpecificMonadSerial m backend,
      MonadSerial (m backend),
      MonadSerial (m (Window backend)),
      MonadSerialWindow m (Window backend))
  => SerialOrigin
  -> Int
  -> Int
  -> m (Window backend) context a
  -> m backend context a
withWindow origin offset length action = do
  absoluteOffset <- case origin of
                      OffsetFromStart -> do
                        return offset
                      OffsetFromEnd -> do
                        oldOffset <- tell
                        seek OffsetFromEnd 0
                        totalLength <- tell
                        seek OffsetFromStart oldOffset
                        return $ offset + totalLength
                      OffsetFromCurrent -> do
                        currentOffset <- tell
                        return $ offset + currentOffset
  monadSerialConstructor $ \internals context tags -> do
    let dataSource = internalsDataSource internals
        window = Window {
                     windowStart = absoluteOffset,
                     windowLength = length
                   }
        windowedDataSource =
          WindowDataSource {
            windowDataSourceUnderlying = dataSource,
            windowDataSourceWindow = window
          }
        windowedInternals =
          windowInternals windowedDataSource internals
    x <- monadSerialActionAccessor action windowedInternals context tags
    case x of
      Left failure -> return $ Left failure
      Right (modifiedWindowedInternals, a) -> do
        let modifiedInternals =
              windowInternalsUnderlying modifiedWindowedInternals
        return $ Right (modifiedInternals, a)
-}


byteStringSeek
  :: (Monad (m ByteString context),
      MonadSerial (m ByteString),
      MonadSerialByteString m)
  => SerialOrigin
  -> Int
  -> m ByteString context ()
byteStringSeek origin
               desiredOffset = do
  internals <- getInternals
  let dataSource = internalsDataSource internals
      byteString = backend dataSource
      totalLength = BS.length byteString
      currentOffset = byteStringInternalsOffset internals
      absoluteDesiredOffset =
        case origin of
          OffsetFromStart -> desiredOffset
          OffsetFromEnd -> desiredOffset + totalLength
          OffsetFromCurrent -> desiredOffset + currentOffset
      newInternals = updateByteStringInternalsOffset
                      absoluteDesiredOffset
                      internals
  if (absoluteDesiredOffset < 0) || (absoluteDesiredOffset > totalLength)
    then throw $ OutOfRangeSerializationFailure absoluteDesiredOffset
    else putInternals newInternals


byteStringTell
  :: (Monad (m ByteString context),
      MonadSerialByteString m)
  => m ByteString context Int
byteStringTell = do
  internals <- getInternals
  return $ byteStringInternalsOffset internals


byteStringIsEOF
  :: (Monad (m ByteString context),
      MonadSerial (m ByteString),
      MonadSerialByteString m)
  => m ByteString context Bool
byteStringIsEOF = do
  offset <- tell
  internals <- getInternals
  let dataSource = internalsDataSource internals
      byteString = backend dataSource
  return $ offset == BS.length byteString


byteStringWrite
  :: (Monad (m ByteString context),
      MonadSerialByteString m,
      MonadSerialWriter m ByteString)
  => ByteString
  -> m ByteString context ()
byteStringWrite output = do
  error "writeByteString not implemented"


byteStringRead
  :: (Monad (m ByteString context),
      MonadSerialByteString m,
      MonadSerialReader m ByteString)
  => Int
  -> m ByteString context ByteString
byteStringRead nBytes = do
  internals <- getInternals
  let dataSource = internalsDataSource internals
      byteString = backend dataSource
      totalLength = BS.length byteString
      currentOffset = byteStringInternalsOffset internals
      actualLengthRead = min nBytes (max 0 (totalLength - currentOffset))
      newOffset = currentOffset + actualLengthRead
      newInternals = updateByteStringInternalsOffset
                      newOffset
                      internals
      result = BS.take actualLengthRead $ BS.drop currentOffset byteString
  putInternals newInternals
  return result


handleSeek
  :: (Monad (m FilePath context),
      MonadSerial (m FilePath),
      MonadSerialIO m FilePath)
  => Handle
  -> SerialOrigin
  -> Int
  -> m FilePath context ()
handleSeek handle origin desiredOffset = do
  let lowLevelOrigin = case origin of
                         OffsetFromStart -> AbsoluteSeek
                         OffsetFromEnd -> SeekFromEnd
                         OffsetFromCurrent -> RelativeSeek
      lowLevelOffset = fromIntegral desiredOffset
  catchIO (hSeek handle lowLevelOrigin lowLevelOffset)
          (\exception -> do
            return (exception :: IOException)
            absoluteDesiredOffset <-
              case origin of
                OffsetFromStart -> return desiredOffset
                OffsetFromEnd -> do
                  seek OffsetFromEnd 0
                  end <- tell
                  return $ desiredOffset + end
                OffsetFromCurrent -> do
                  current <- tell
                  return $ desiredOffset + current
            internals <- getInternals
            throw $ OutOfRangeSerializationFailure absoluteDesiredOffset)


handleTell
  :: (Monad (m FilePath context),
      MonadSerial (m FilePath),
      MonadSerialIO m FilePath)
  => Handle
  -> m FilePath context Int
handleTell handle = do
  catchIO (do
            result <- hTell handle
            return $ fromIntegral result)
          (\exception -> throw $ LowLevelSerializationFailure exception)


handleIsEOF
  :: (Monad (m FilePath context),
      MonadSerial (m FilePath),
      MonadSerialIO m FilePath)
  => Handle
  -> m FilePath context Bool
handleIsEOF handle = do
  catchIO (hIsEOF handle)
          (\exception -> throw $ LowLevelSerializationFailure exception)


handleWrite
  :: (Monad (m FilePath context),
      MonadSerial (m FilePath),
      MonadSerialIO m FilePath,
      MonadSerialWriter m FilePath)
  => Handle
  -> ByteString
  -> m FilePath context ()
handleWrite handle output = do
  catchIO (BS.hPut handle output)
          (\exception -> throw $ LowLevelSerializationFailure exception)


handleRead
  :: (Monad (m FilePath context),
      MonadSerial (m FilePath),
      MonadSerialIO m FilePath,
      MonadSerialReader m FilePath)
  => Handle
  -> Int
  -> m FilePath context ByteString
handleRead handle nBytes = do
  catchIO (BS.hGet handle nBytes)
          (\exception -> throw $ LowLevelSerializationFailure exception)


windowRunInUnderlying
  :: (BackendSpecificMonadSerial m (Window underlying),
      BackendSpecificMonadSerial m underlying,
      MonadSerialWindow m (Window underlying))
  => m underlying context a
  -> m (Window underlying) context a
windowRunInUnderlying x = undefined -- TODO
{-
windowRunInUnderlying x =
  monadSerialConstructor $ \internals context tags -> do
    let underlyingInternals =
          windowInternalsUnderlying internals
    v <- monadSerialActionAccessor x underlyingInternals context tags
    case v of
      Left failure -> return $ Left failure
      Right (underlyingInternals', y) -> do
        let internals' =
              updateWindowInternalsUnderlying internals underlyingInternals'
        return $ Right (internals', y)
-}


windowSeek
  :: (Monad (m (Window underlying) context),
      BackendSpecificMonadSerial m underlying,
      MonadSerial (m (Window underlying)),
      MonadSerial (m underlying),
      MonadSerialWindow m (Window underlying))
  => SerialOrigin
  -> Int
  -> m (Window underlying) context ()
windowSeek origin desiredOffset = do
  internals <- getInternals
  let dataSource = internalsDataSource internals
      window = backend dataSource
  absoluteDesiredOffset <- case origin of
                             OffsetFromStart -> return desiredOffset
                             OffsetFromEnd -> do
                               return $ desiredOffset + windowLength window
                             OffsetFromCurrent -> do
                               currentOffset <- tell
                               return $ desiredOffset + currentOffset
  if (absoluteDesiredOffset < 0)
     || (absoluteDesiredOffset > windowLength window)
    then throw $ OutOfRangeSerializationFailure absoluteDesiredOffset
    else do
      let underlyingDesiredOffset = absoluteDesiredOffset + windowStart window
      runInUnderlying $ seek OffsetFromStart underlyingDesiredOffset


windowTell
  :: (Monad (m (Window underlying) context),
      BackendSpecificMonadSerial m underlying,
      MonadSerial (m (Window underlying)),
      MonadSerial (m underlying),
      MonadSerialWindow m (Window underlying))
  => m (Window underlying) context Int
windowTell = do
  underlyingOffset <- runInUnderlying tell
  internals <- getInternals
  let dataSource = internalsDataSource internals
      window = backend dataSource
      maybeFailure =
        if (underlyingOffset < windowStart window)
           || (underlyingOffset - windowStart window > windowLength window)
          then Just $ OutOfRangeSerializationFailure offset
          else Nothing
      offset = underlyingOffset - windowStart window
  case maybeFailure of
    Just failure -> throw failure
    Nothing -> return offset


windowIsEOF
  :: (Monad (m (Window underlying) context),
      MonadSerial (m (Window underlying)),
      MonadSerial (m underlying),
      MonadSerialWindow m (Window underlying))
  => m (Window underlying) context Bool
windowIsEOF = do
  offset <- tell
  internals <- getInternals
  let dataSource = internalsDataSource internals
      window = backend dataSource
  return $ offset == windowLength window


windowWrite
  :: (Monad (m (Window underlying) context),
      MonadSerialWriter m (Window underlying))
  => ByteString
  -> m (Window underlying) context ()
windowWrite output = error "writeWindow not implemented"


windowRead
  :: (Monad (m (Window underlying) context),
      MonadSerial (m (Window underlying)),
      MonadSerialReader m underlying,
      MonadSerialWindow m (Window underlying))
  => Int
  -> m (Window underlying) context ByteString
windowRead nBytes = do
  offset <- tell
  internals <- getInternals
  let dataSource = internalsDataSource internals
      window = backend dataSource
      totalLength = windowLength window
  if offset + nBytes <= totalLength
    then runInUnderlying $ read nBytes
    else throw $ InsufficientDataSerializationFailure nBytes


runSerializationToByteString
  :: BackendSpecificSerialization ByteString () a
  -> Either (Int, [(Int, String)], SomeSerializationFailure) (a, ByteString)
runSerializationToByteString action =
  error "runSerializationToByteString not implemented"


runSerializationToFile
  :: BackendSpecificSerialization FilePath () a
  -> FilePath
  -> IO (Either (Int, [(Int, String)], SomeSerializationFailure) a)
runSerializationToFile action filePath =
  error "runSerializationToFile not implemented"


runDeserializationFromByteString
  :: BackendSpecificDeserialization ByteString () a
  -> ByteString
  -> Either (Int, [(Int, String)], SomeSerializationFailure) a
runDeserializationFromByteString action byteString =
  identityAction $ do
    let dataSource =
          ByteStringSerialDataSource {
              byteStringSerialDataSourceByteString = byteString
            }
        internals =
          ByteStringDeserializationInternals {
              byteStringDeserializationInternalsDataSource = dataSource,
              byteStringDeserializationInternalsOffset = 0
            }
        context = ()
        tags = []
    result <- deserializationAction action internals context tags
    case result of
      Left failure -> return $ Left failure
      Right (_, result) -> return $ Right result


runDeserializationFromFile
  :: BackendSpecificDeserialization FilePath () a
  -> FilePath
  -> IO (Either (Int, [(Int, String)], SomeSerializationFailure) a)
runDeserializationFromFile action filePath = do
  withBinaryFile filePath ReadMode $ \handle -> do
    let dataSource =
          FilePathSerialDataSource {
              filePathSerialDataSourceFilePath = filePath
            }
        internals =
          FilePathDeserializationInternals {
              filePathDeserializationInternalsDataSource = dataSource,
              filePathDeserializationInternalsHandle = handle
            }
        context = ()
        tags = []
    result <- deserializationAction action internals context tags
    case result of
      Left failure -> return $ Left failure
      Right (_, result) -> return $ Right result


runSubDeserializationFromByteString
  :: BackendSpecificDeserialization ByteString () a
  -> ByteString
  -> Deserialization a
runSubDeserializationFromByteString action byteString =
  case runDeserializationFromByteString action byteString of
    Left (_, _, failure) -> throw failure
    Right result -> return result


toByteString
  :: Serializable () a
  => a
  -> (Either (Int, [(Int, String)], SomeSerializationFailure) ByteString)
toByteString value =
  case runSerializationToByteString (serialize value) of
    Left failure -> Left failure
    Right (_, byteString) -> Right byteString


toFile
  :: Serializable () a
  => a
  -> FilePath
  -> IO (Maybe (Int, [(Int, String)], SomeSerializationFailure))
toFile value filePath = do
  result <- runSerializationToFile (serialize value) filePath
  case result of
    Left failure -> return (Just failure)
    Right _ -> return Nothing


fromByteString
  :: Serializable () a
  => ByteString
  -> Either (Int, [(Int, String)], SomeSerializationFailure) a
fromByteString byteString =
  runDeserializationFromByteString deserialize byteString


fromFile
  :: Serializable () a
  => FilePath
  -> IO (Either (Int, [(Int, String)], SomeSerializationFailure) a)
fromFile filePath =
  runDeserializationFromFile deserialize filePath


serializeWord
  :: (Bits word, Integral word, Num word, HasEndianness context)
  => word
  -> ContextualSerialization context ()
serializeWord word = do
  context <- getContext
  let byteSize = div (bitSize word) 8
      getByte byteIndex =
        fromIntegral $ 0xFF .&. shiftR word (byteIndex * 8)
      byteSequence = case considerEndianness context of
                       LittleEndian -> [0 .. byteSize - 1]
                       BigEndian -> [byteSize - 1, byteSize - 2 .. 0]
  write $ BS.pack $ map getByte byteSequence


deserializeWord
  :: forall word context
  .  (Bits word, Integral word, Num word, HasEndianness context)
  => ContextualDeserialization context word
deserializeWord = do
  context <- getContext
  let byteSize = div (bitSize (0 :: word)) 8
      combine byteString =
        foldl' (.|.)
               0
               $ zipWith (\byteIndex byte ->
                            shiftL (fromIntegral byte)
                                   (byteIndex * 8))
                         byteSequence
                         (BS.unpack byteString)
      byteSequence = case considerEndianness context of
                       LittleEndian -> [0 .. byteSize - 1]
                       BigEndian -> [byteSize - 1, byteSize - 2 .. 0]
  byteString <- read byteSize
  return $ combine byteString


serializeNullTerminatedText :: ByteString -> Serialization ()
serializeNullTerminatedText text = do
  write text
  write $ BS.pack [0x00]


deserializeNullTerminatedText :: Deserialization ByteString
deserializeNullTerminatedText = do
  let loop octetsSoFar = do
        octetByteString <- read 1
        let octet = BS.head octetByteString
        if octet == 0x00
          then return $ BS.pack octetsSoFar
          else loop $ octetsSoFar ++ [octet]
  loop []


serializeNullPaddedText :: Int -> ByteString -> Serialization ()
serializeNullPaddedText paddedLength text = do
  write text
  write $ BS.pack $ take (paddedLength - BS.length text) $ repeat 0x00


deserializeNullPaddedText :: Int -> Deserialization ByteString
deserializeNullPaddedText paddedLength = do
  byteString <- read paddedLength
  return
    $ BS.reverse
       $ BS.dropWhile (\octet -> octet == 0x00)
          $ BS.reverse byteString

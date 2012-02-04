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
            MonadSerialWriter (BackendSpecificSerialization backend))
        => BackendSpecificSerialization backend context a
    }


newtype ContextualDeserialization context a =
  ContextualDeserialization {
      contextualDeserializationAction
        :: forall backend
        .  (BackendSpecificMonadSerial BackendSpecificDeserialization backend,
            MonadSerial (BackendSpecificDeserialization backend),
            MonadSerialReader (BackendSpecificDeserialization backend))
        => BackendSpecificDeserialization backend context a
    }


data SerialOrigin
  = OffsetFromStart
  | OffsetFromCurrent
  | OffsetFromEnd
  deriving (Eq, Ord, Show)


data Window
  = IdentityWindow
  | StackedWindow {
        stackedWindowStart :: Int,
        stackedWindowLength :: Int,
        stackedWindowUnderlying :: Window
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
        -> Window
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
        -> Window
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


class MonadSerial m where
  getContext
    :: forall context
    .  (Monad (m context))
    => m context context
  withContext
    :: forall context context' a
    .  (Monad (m context),
        Monad (m context'))
    => context'
    -> m context' a
    -> m context a
  
  getTags
    :: forall context
    .  (Monad (m context))
    => m context [(Int, String)]
  withTag
    :: forall context a
    .  (Monad (m context))
    => String
    -> m context a
    -> m context a
  
  getWindow
    :: forall context
    .  (Monad (m context))
    => m context Window
  withWindow
    :: forall context a
    .  (Monad (m context))
    => SerialOrigin
    -> Int
    -> Int
    -> m context a
    -> m context a
  
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


class MonadSerial m => MonadSerialReader m where
  read :: Int -> m context ByteString


class MonadSerial m => MonadSerialWriter m where
  write :: ByteString -> m context ()


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
  ioInternalsHandle
    :: Internals m backend
    -> Handle
  
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
  backend = byteStringSerialDataSourceByteString


instance Serial FilePath where
  data SerialDataSource FilePath =
    FilePathSerialDataSource {
        filePathSerialDataSourceFilePath :: FilePath
      }
  type PrimitiveMonad FilePath = IO
  backend = filePathSerialDataSourceFilePath


instance Monad Identity where
  return a = Identity a
  (Identity x) >>= f = f x


instance Monad (PrimitiveMonad backend)
         => Monad (BackendSpecificSerialization backend context) where
  return a = BackendSpecificSerialization $ \internals _ _ _ ->
               return $ Right (internals, a)
  (BackendSpecificSerialization x) >>= f =
    BackendSpecificSerialization $ \internals context tags window -> do
      v <- x internals context tags window
      case v of
        Left failure -> return $ Left failure
        Right (internals', y) ->
          serializationAction (f y) internals' context tags window


instance Monad (PrimitiveMonad backend)
         => Monad (BackendSpecificDeserialization backend context) where
  return a = BackendSpecificDeserialization $ \internals _ _ _ ->
    return $ Right (internals, a)
  (BackendSpecificDeserialization x) >>= f =
    BackendSpecificDeserialization $ \internals context tags window -> do
      v <- x internals context tags window
      case v of
        Left failure -> return $ Left failure
        Right (internals', y) ->
          deserializationAction (f y) internals' context tags window


instance forall context
         . Monad (ContextualSerialization context) where
  return a = ContextualSerialization $ return a
  x >>= f =
    ContextualSerialization $ do
      v <- contextualSerializationAction x
      contextualSerializationAction $ f v


instance forall context
         . Monad (ContextualDeserialization context) where
  return a = ContextualDeserialization $ return a
  x >>= f =
    ContextualDeserialization $ do
      v <- contextualDeserializationAction x
      contextualDeserializationAction $ f v


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
    BackendSpecificSerialization $ \internals _ _ _ ->
      return $ Right (internals, internals)
  putInternals internals =
    BackendSpecificSerialization $ \_ _ _ _ ->
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
    BackendSpecificSerialization $ \internals _ _ _ ->
      return $ Right (internals, internals)
  putInternals internals =
    BackendSpecificSerialization $ \_ _ _ _ ->
      return $ Right (internals, ())
  internalsDataSource = filePathSerializationInternalsDataSource


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
    BackendSpecificDeserialization $ \internals _ _ _ ->
      return $ Right (internals, internals)
  putInternals internals =
    BackendSpecificDeserialization $ \_ _ _ _ ->
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
    BackendSpecificDeserialization $ \internals _ _ _ ->
      return $ Right (internals, internals)
  putInternals internals =
    BackendSpecificDeserialization $ \_ _ _ _ ->
      return $ Right (internals, ())
  internalsDataSource = filePathDeserializationInternalsDataSource


instance MonadSerial (BackendSpecificSerialization ByteString) where  
  getContext = getContextImplementation BackendSpecificSerialization
  withContext context action =
    withContextImplementation BackendSpecificSerialization
                              serializationAction
                              context action
  
  getTags = getTagsImplementation BackendSpecificSerialization
  withTag tag action =
    withTagImplementation BackendSpecificSerialization serializationAction
                          tag action
  
  getWindow = getWindowImplementation BackendSpecificSerialization
  withWindow = withWindowImplementation BackendSpecificSerialization
                                        serializationAction
  
  throw failure = throwImplementation BackendSpecificSerialization failure
  catch action handler =
    catchImplementation BackendSpecificSerialization serializationAction
                        action handler
  seek = seekImplementation byteStringSeek
  tell = tellImplementation byteStringTell
  primitiveTell = primitiveTellImplementation byteStringTell
  isEOF = isEOFImplementation byteStringIsEOF


instance MonadSerial (BackendSpecificSerialization FilePath) where
  getContext = getContextImplementation BackendSpecificSerialization
  withContext context action =
    withContextImplementation BackendSpecificSerialization
                              serializationAction
                              context action
  
  getTags = getTagsImplementation BackendSpecificSerialization
  withTag tag action =
    withTagImplementation BackendSpecificSerialization serializationAction
                          tag action
  
  getWindow = getWindowImplementation BackendSpecificSerialization
  withWindow = withWindowImplementation BackendSpecificSerialization
                                        serializationAction
  throw failure = throwImplementation BackendSpecificSerialization failure
  catch action handler =
    catchImplementation BackendSpecificSerialization serializationAction
                        action handler
  
  seek = seekImplementation handleSeek
  tell = tellImplementation handleTell
  primitiveTell = primitiveTellImplementation handleTell
  isEOF = isEOFImplementation handleIsEOF


instance MonadSerial (BackendSpecificDeserialization ByteString)
         where
  getContext = getContextImplementation BackendSpecificDeserialization
  withContext context action =
    withContextImplementation BackendSpecificDeserialization
                              deserializationAction
                              context action
  
  getTags = getTagsImplementation BackendSpecificDeserialization
  withTag tag action =
    withTagImplementation BackendSpecificDeserialization deserializationAction
                          tag action
  
  getWindow = getWindowImplementation BackendSpecificDeserialization
  withWindow = withWindowImplementation BackendSpecificDeserialization
                                        deserializationAction
  
  throw failure = throwImplementation BackendSpecificDeserialization failure
  catch action handler =
    catchImplementation BackendSpecificDeserialization deserializationAction
                        action handler
  
  seek = seekImplementation byteStringSeek
  tell = tellImplementation byteStringTell
  primitiveTell = primitiveTellImplementation byteStringTell
  isEOF = isEOFImplementation byteStringIsEOF


instance MonadSerial (BackendSpecificDeserialization FilePath) where
  getContext = getContextImplementation BackendSpecificDeserialization
  withContext context action =
    withContextImplementation BackendSpecificDeserialization
                              deserializationAction
                              context action
  
  getTags = getTagsImplementation BackendSpecificDeserialization
  withTag tag action =
    withTagImplementation BackendSpecificDeserialization deserializationAction
                          tag action
  
  getWindow = getWindowImplementation BackendSpecificDeserialization
  withWindow = withWindowImplementation BackendSpecificDeserialization
                                        deserializationAction
  
  throw failure = throwImplementation BackendSpecificDeserialization failure
  catch action handler =
    catchImplementation BackendSpecificDeserialization deserializationAction
                        action handler
  
  seek = seekImplementation handleSeek
  tell = tellImplementation handleTell
  primitiveTell = primitiveTellImplementation handleTell
  isEOF = isEOFImplementation handleIsEOF


instance MonadSerial ContextualSerialization where
  getContext = ContextualSerialization $ getContext
  withContext context action =
    ContextualSerialization
     $ withContext context $ contextualSerializationAction action
  
  getTags = ContextualSerialization $ getTags
  withTag tag action =
    ContextualSerialization
     $ withTag tag $ contextualSerializationAction action
  
  getWindow = ContextualSerialization $ getWindow
  withWindow origin offset length action =
    ContextualSerialization
      $ withWindow origin offset length
                   $ contextualSerializationAction action
  
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
  getContext = ContextualDeserialization $ getContext
  withContext context action =
    ContextualDeserialization
     $ withContext context $ contextualDeserializationAction action
  
  getTags = ContextualDeserialization $ getTags
  withTag tag action =
    ContextualDeserialization
     $ withTag tag $ contextualDeserializationAction action
  
  getWindow = ContextualDeserialization $ getWindow
  withWindow origin offset length action =
    ContextualDeserialization
      $ withWindow origin offset length
                   $ contextualDeserializationAction action
  
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


instance MonadSerialWriter (BackendSpecificSerialization ByteString) where
  write = writeImplementation byteStringWrite


instance MonadSerialWriter (BackendSpecificSerialization FilePath) where
  write = writeImplementation handleWrite


instance MonadSerialWriter ContextualSerialization where
  write byteString = ContextualSerialization $ write byteString


instance MonadSerialReader (BackendSpecificDeserialization ByteString) where
  read = readImplementation byteStringRead
      

instance MonadSerialReader (BackendSpecificDeserialization FilePath) where
  read = readImplementation handleRead


instance MonadSerialReader ContextualDeserialization where
  read nBytes = ContextualDeserialization $ read nBytes


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
  ioInternalsHandle = filePathSerializationInternalsHandle
  
  catchIO action handler =
    BackendSpecificSerialization $ \internals context tags window -> do
      E.catch (do
                 result <- action
                 return $ Right (internals, result))
              (\exception ->
                 serializationAction (handler exception)
                                     internals context tags window)


instance MonadSerialIO BackendSpecificDeserialization
                       FilePath
                       where
  ioInternalsHandle = filePathDeserializationInternalsHandle
  
  catchIO action handler =
    BackendSpecificDeserialization $ \internals context tags window -> do
      E.catch (do
                 result <- action
                 return $ Right (internals, result))
              (\exception ->
                 deserializationAction (handler exception)
                                       internals context tags window)


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


throwImplementation
  :: (Monad (m context),
      Monad m',
      MonadSerial m,
      SerializationFailure failure)
  => ((internals
       -> context
       -> [(Int, String)]
       -> Window
       -> m' (Either (Int, [(Int, String)], SomeSerializationFailure)
                     (internals, a)))
      -> m context a)
  -> failure
  -> m context a
throwImplementation constructor failure = do
  offset <- primitiveTell
  constructor $ \_ _ tags _ ->
    return $ Left (offset, tags, toSerializationFailure failure)


catchImplementation
  :: (Monad (m context),
      Monad m',
      MonadSerial m,
      SerializationFailure failure)
  => ((internals
       -> context
       -> [(Int, String)]
       -> Window
       -> m' (Either (Int, [(Int, String)], SomeSerializationFailure)
                     (internals, a)))
      -> m context a)
  -> (m context a
      -> internals
      -> context
      -> [(Int, String)]
      -> Window
      -> m' (Either (Int, [(Int, String)], SomeSerializationFailure)
                    (internals, a)))
  -> m context a
  -> (Int
      -> [(Int, String)]
      -> failure
      -> m context a)
  -> m context a
catchImplementation constructor accessor action handler = do
  initialOffset <- tell
  constructor $ \internals context tags window -> do
    result <- accessor action internals context tags window
    case result of
      Left (failureOffset, failureTags, failure) ->
        case fromSerializationFailure failure of
          Nothing -> return result
          Just specificFailure ->
            accessor
             (do
               seek OffsetFromStart initialOffset
               handler failureOffset failureTags specificFailure)
             internals context tags window
      Right _ -> return result


getContextImplementation
  :: (Monad (m context),
      Monad m')
  => ((internals
       -> context
       -> [(Int, String)]
       -> Window
       -> m' (Either (Int, [(Int, String)], SomeSerializationFailure)
                     (internals, context)))
      -> m context context)
  -> m context context
getContextImplementation constructor = do
  constructor $ \internals context _ _ ->
    return $ Right (internals, context)


withContextImplementation
  :: (Monad (m context),
      Monad m',
      MonadSerial m)
  => ((internals
       -> context
       -> [(Int, String)]
       -> Window
       -> m' (Either (Int, [(Int, String)], SomeSerializationFailure)
                     (internals, a)))
      -> m context a)
  -> (m context' a
      -> internals
      -> context'
      -> [(Int, String)]
      -> Window
      -> m' (Either (Int, [(Int, String)], SomeSerializationFailure)
                    (internals, a)))
  -> context'
  -> m context' a
  -> m context a
withContextImplementation constructor accessor context x = do
  constructor $ \internals _ tags window -> do
    v <- accessor x internals context tags window
    case v of
      Left failure -> return $ Left failure
      Right (internals', y) -> return $ Right (internals', y)


getTagsImplementation
  :: (Monad (m context),
      Monad m',
      MonadSerial m)
  => ((internals
       -> context
       -> [(Int, String)]
       -> Window
       -> m' (Either (Int, [(Int, String)], SomeSerializationFailure)
                     (internals, [(Int, String)])))
      -> m context [(Int, String)])
  -> m context [(Int, String)]
getTagsImplementation constructor =
  constructor $ \internals _ tags _ ->
    return $ Right (internals, tags)


withTagImplementation
  :: (Monad (m context),
      Monad m',
      MonadSerial m)
  => ((internals
       -> context
       -> [(Int, String)]
       -> Window
       -> m' (Either (Int, [(Int, String)], SomeSerializationFailure)
                     (internals, a)))
      -> m context a)
  -> (m context a
      -> internals
      -> context
      -> [(Int, String)]
      -> Window
      -> m' (Either (Int, [(Int, String)], SomeSerializationFailure)
                    (internals, a)))
  -> String
  -> m context a
  -> m context a
withTagImplementation constructor accessor tagText action = do
  tagOffset <- primitiveTell
  tags <- getTags
  let tags' = (tagOffset, tagText) : tags
  constructor $ \internals context _ window ->
    accessor action internals context tags' window


getWindowImplementation
  :: (Monad (m context),
      Monad m',
      MonadSerial m)
  => ((internals
       -> context
       -> [(Int, String)]
       -> Window
       -> m' (Either (Int, [(Int, String)], SomeSerializationFailure)
                     (internals, Window)))
      -> m context Window)
  -> m context Window
getWindowImplementation constructor = do
  constructor $ \internals _ _ window -> return $ Right (internals, window)


withWindowImplementation
  :: (Monad (m backend context),
      Monad m',
      MonadSerial (m backend),
      BackendSpecificMonadSerial m backend)
  => ((Internals m backend
       -> context
       -> [(Int, String)]
       -> Window
       -> m' (Either (Int, [(Int, String)], SomeSerializationFailure)
                     (Internals m backend, a)))
      -> m backend context a)
  -> (m backend context a
      -> Internals m backend
      -> context
      -> [(Int, String)]
      -> Window
      -> m' (Either (Int, [(Int, String)], SomeSerializationFailure)
                    (Internals m backend, a)))
  -> SerialOrigin
  -> Int
  -> Int
  -> m backend context a
  -> m backend context a
withWindowImplementation constructor accessor origin offset length action = do
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
  constructor $ \internals context tags underlyingWindow -> do
    let dataSource = internalsDataSource internals
        window = StackedWindow {
                     stackedWindowStart = absoluteOffset,
                     stackedWindowLength = length,
                     stackedWindowUnderlying = underlyingWindow
                   }
    x <- accessor action internals context tags window
    case x of
      Left failure -> return $ Left failure
      Right (internals, a) -> do
        return $ Right (internals, a)


byteStringSeek
  :: (Monad (m ByteString context),
      MonadSerial (m ByteString),
      MonadSerialByteString m)
  => SerialOrigin
  -> Int
  -> m ByteString context ()
byteStringSeek origin desiredOffset = do
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
      MonadSerialWriter (m ByteString))
  => ByteString
  -> m ByteString context ()
byteStringWrite output = do
  undefined


byteStringRead
  :: (Monad (m ByteString context),
      MonadSerialByteString m,
      MonadSerialReader (m ByteString))
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
  => SerialOrigin
  -> Int
  -> m FilePath context ()
handleSeek origin desiredOffset = do
  internals <- getInternals
  let handle = ioInternalsHandle internals
      lowLevelOrigin = case origin of
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
            throw $ OutOfRangeSerializationFailure absoluteDesiredOffset)


handleTell
  :: (Monad (m FilePath context),
      MonadSerial (m FilePath),
      MonadSerialIO m FilePath)
  => m FilePath context Int
handleTell = do
  internals <- getInternals
  let handle = ioInternalsHandle internals
  catchIO (do
            result <- hTell handle
            return $ fromIntegral result)
          (\exception -> throw $ LowLevelSerializationFailure exception)


handleIsEOF
  :: (Monad (m FilePath context),
      MonadSerial (m FilePath),
      MonadSerialIO m FilePath)
  => m FilePath context Bool
handleIsEOF = do
  internals <- getInternals
  let handle = ioInternalsHandle internals
  catchIO (hIsEOF handle)
          (\exception -> throw $ LowLevelSerializationFailure exception)


handleWrite
  :: (Monad (m FilePath context),
      MonadSerial (m FilePath),
      MonadSerialIO m FilePath,
      MonadSerialWriter (m FilePath))
  => ByteString
  -> m FilePath context ()
handleWrite output = do
  internals <- getInternals
  let handle = ioInternalsHandle internals
  catchIO (BS.hPut handle output)
          (\exception -> throw $ LowLevelSerializationFailure exception)


handleRead
  :: (Monad (m FilePath context),
      MonadSerial (m FilePath),
      MonadSerialIO m FilePath,
      MonadSerialReader (m FilePath))
  => Int
  -> m FilePath context ByteString
handleRead nBytes = do
  internals <- getInternals
  let handle = ioInternalsHandle internals
  catchIO (BS.hGet handle nBytes)
          (\exception -> throw $ LowLevelSerializationFailure exception)


seekImplementation
  :: (Monad (m backend context),
      MonadSerial (m backend),
      BackendSpecificMonadSerial m backend)
  => (SerialOrigin
      -> Int
      -> m backend context a)
  -> SerialOrigin
  -> Int
  -> m backend context a
seekImplementation backendSeek origin desiredOffset = do
  recurseOnWindows
    (\(origin, desiredOffset) -> backendSeek origin desiredOffset)
    (\(origin, desiredOffset) windowStart windowLength recurse -> do
       internals <- getInternals
       let dataSource = internalsDataSource internals
           window = backend dataSource
       absoluteDesiredOffset <- case origin of
                                  OffsetFromStart -> return desiredOffset
                                  OffsetFromEnd -> do
                                    return $ desiredOffset + windowLength
                                  OffsetFromCurrent -> do
                                    currentOffset <- tell
                                    return $ desiredOffset + currentOffset
       if (absoluteDesiredOffset < 0)
          || (absoluteDesiredOffset > windowLength)
         then throw $ OutOfRangeSerializationFailure absoluteDesiredOffset
         else do
           let underlyingDesiredOffset =
                 absoluteDesiredOffset + windowStart
           recurse (OffsetFromStart, underlyingDesiredOffset))
    (origin, desiredOffset)


tellImplementation
  :: (Monad (m backend context),
      MonadSerial (m backend),
      BackendSpecificMonadSerial m backend)
  => m backend context Int
  -> m backend context Int
tellImplementation backendTell = do
  recurseOnWindows
    (\() -> backendTell)
    (\() windowStart windowLength recurse -> do
      underlyingOffset <- recurse ()
      internals <- getInternals
      let dataSource = internalsDataSource internals
          maybeFailure =
            if (underlyingOffset < windowStart)
               || (underlyingOffset - windowStart > windowLength)
              then Just $ OutOfRangeSerializationFailure offset
              else Nothing
          offset = underlyingOffset - windowStart
      case maybeFailure of
        Just failure -> throw failure
        Nothing -> return offset)
    ()


primitiveTellImplementation
  :: (Monad (m backend context),
      MonadSerial (m backend),
      BackendSpecificMonadSerial m backend)
  => m backend context Int
  -> m backend context Int
primitiveTellImplementation backendTell = do
  recurseOnWindows
    (\() -> backendTell)
    (\() _ _ recurse -> recurse ())
    ()


isEOFImplementation
  :: (Monad (m backend context),
      MonadSerial (m backend),
      BackendSpecificMonadSerial m backend)
  => m backend context Bool
  -> m backend context Bool
isEOFImplementation backendIsEOF = do
  recurseOnWindows
    (\() -> backendIsEOF)
    (\() _ windowLength _ -> do
       offset <- tell
       internals <- getInternals
       return $ offset == windowLength)
    ()


writeImplementation
  :: (MonadSerialWriter m)
  => (ByteString -> m context ())
  -> ByteString
  -> m context ()
writeImplementation backendWrite output = do
  undefined


readImplementation
  :: (Monad (m context),
      MonadSerialReader m)
  => (Int -> m context ByteString)
  -> Int
  -> m context ByteString
readImplementation backendRead nBytes = do
  recurseOnWindows
    (\_ -> backendRead nBytes)
    (\maybeOffset _ windowLength recurse -> do
      offset <- case maybeOffset of
                  Nothing -> tell
                  Just offset -> return offset
      if offset + nBytes <= windowLength
        then recurse $ Just offset
        else throw $ InsufficientDataSerializationFailure nBytes)
    Nothing


recurseOnWindows
  :: (Monad (m context),
      MonadSerial m)
  => (b -> m context a)
  -> (b -> Int -> Int -> (b -> m context a) -> m context a)
  -> b
  -> m context a
recurseOnWindows baseCase recursiveCase initialValue = do
  let loop window value = do
        case window of
          IdentityWindow -> baseCase value
          StackedWindow { } -> do
            let windowStart = stackedWindowStart window
                windowLength = stackedWindowLength window
                underlyingWindow = stackedWindowUnderlying window
            recursiveCase value windowStart windowLength
                          $ loop underlyingWindow
  outermostWindow <- getWindow
  loop outermostWindow initialValue


runSerializationToByteString
  :: ContextualSerialization () a
  -> Either (Int, [(Int, String)], SomeSerializationFailure) (a, ByteString)
runSerializationToByteString action = do
  undefined


runSerializationToFile
  :: ContextualSerialization () a
  -> FilePath
  -> IO (Either (Int, [(Int, String)], SomeSerializationFailure) a)
runSerializationToFile action filePath = do
  undefined


runDeserializationFromByteString
  :: ContextualDeserialization () a
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
        window = IdentityWindow
    result <- deserializationAction
               (contextualDeserializationAction action)
               internals context tags window
    case result of
      Left failure -> return $ Left failure
      Right (_, result) -> return $ Right result


runDeserializationFromFile
  :: ContextualDeserialization () a
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
        window = IdentityWindow
    result <- deserializationAction
               (contextualDeserializationAction action)
               internals context tags window
    case result of
      Left failure -> return $ Left failure
      Right (_, result) -> return $ Right result


runSubDeserializationFromByteString
  :: ContextualDeserialization () a
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

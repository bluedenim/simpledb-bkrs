package org.van;

import org.apache.log4j.Logger;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Utility for retry operations
 *
 * Created by vly on 11/8/2015.
 */
public class RetryUtility {

   private static final Logger logger =
      Logger.getLogger(RetryUtility.class);

   /**
    * Exception thrown when the maximum number of retries have been spent for a work.
    */
   public static class RetryExhaustedException extends RuntimeException {

      private RetryExhaustedException(String message, Throwable t) {
         super(message, t);
      }
   }

   @FunctionalInterface
   public interface Work<T,U> {
      /**
       * Perform work and return a value. The 0-based trial number (if retrying) is provided.
       *
       * @param t the caller-supplied value
       * @param trial the trail number (0-based)
       *
       * @return return value for the worker
       * @throws RuntimeException
       */
      U perform(T t, int trial) throws RuntimeException;
   }

   /**
    * perform the work (embodied by the provided {@link Supplier}) and retry as necessary up
    * to the number of retries supplied. If the number of retries exceeded the maximum,
    *
    *
    * @param work the supplier work implementation (either {@link Work} or any compatible
    *     {@link java.util.function.BiFunction}
    * @param workParam the parameter/argument to be passed to the worker
    * @param maxRetries the maximum number of retries (>= 0)
    * @param <T>
    * @param <U>
    *
    * @return the value from the supplier provided
    *
    * @throws RetryExhaustedException
    */
   public static <T,U> U performWithRetry(Work<T,U> work, T workParam, int maxRetries) {
      Objects.requireNonNull(work);
      int trial = 1;
      U result = null;
      if (maxRetries < 0) {
         maxRetries = 0;
      }
      boolean done = false;
      while (!done) {
         logger.debug(String.format("Performing work (trial #%d)", trial));
         try {
            result = work.perform(workParam, trial);
            done = true;
         } catch (Exception ex) {
                String msg;
                if ((trial - 1) == maxRetries) {
                    throw new RetryExhaustedException("Exception encountered after exhausted retries.", ex);
                } else {
                    trial++;
                    msg = "Exception encountered. Retrying.";
                    logger.warn(msg, ex);
                }
         }
      }
      return result;
   }
}

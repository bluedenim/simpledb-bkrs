package org.van.providers;

import com.amazonaws.services.simpledb.model.Item;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;

/**
 * Abstract class for a source of {@link Item}. Users should call {@link #initialize()} before
 * using the instance, call {@link #iterateItems(Consumer)} to iterate over the items, and
 * lastly call {@link #close()} when done with the instance.
 *
 * Created by vly on 11/15/2015.
 */
public abstract class ItemSourceProvider implements Closeable {

    /**
     * Initialize the instance. Call this before using this instance.
     *
     * @return this instance
     */
    public ItemSourceProvider initialize() {
        return this;
    }

    /**
     * Iterate over the items of this source. The provided {@link Consumer} will be
     * called for each item iterated.
     *
     * @param consumer the {@link Consumer} of {@link Item}s iterated
     */
    public abstract void iterateItems(Consumer<Item> consumer);

    /**
     * Clean up and close any resources.
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
    }
}

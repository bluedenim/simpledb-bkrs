package org.van.providers;

import com.amazonaws.services.simpledb.model.Item;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Created by vly on 11/15/2015.
 */
public abstract class ItemSourceProvider implements Closeable {

    public ItemSourceProvider initialize() {
        return this;
    }

    public abstract void iterateItems(Consumer<Item> consumer);

    @Override
    public void close() throws IOException {
    }
}

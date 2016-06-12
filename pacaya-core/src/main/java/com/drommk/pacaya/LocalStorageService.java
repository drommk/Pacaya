package com.drommk.pacaya;

import java.util.List;

import rx.Observable;

public interface LocalStorageService<T> {
    /**
     * retrieve up to <code>limit</code> {@link T} items from local storage
     *
     * @param limit max number of results
     * @return a limited list of {@link T} items, ordered by age
     */
    Observable<List<T>> list(int limit);


    /**
     * Counts the number of items currently in storage.
     *
     * @return a Long {@link Observable} representing that count
     */
    Observable<Integer> count();

    /**
     * Insert a {@link T} item into storage
     *
     * @param items the list of {@link T} items that will be logged
     * @return the last inserted {@link T} item
     */
    Observable<T> insert(List<T> items);


    /**
     * Delete all {@link T} items which are older than <code>eventId</code> (included)
     *
     * @param eventId represents the last id that needs to be deleted.
     * @return the number of deleted items
     */
    Observable<Integer> deleteUpTo(String eventId);

    /**
     * Delete the <code>count</code> older items from local storage
     *
     * @param count number of items to delete
     * @return the number of deleted items
     */
    Observable<Integer> deleteOlderItems(Integer count);
}
package com.drommk.pacaya;

import java.util.List;

/**
 * Created by ericpalle on 3/17/16.
 */
public interface ReportService<T> {

    String BUFFER_FULL = "buffer_full";
    String BATCH_REJECTED = "batch_rejected";

    /**
     * Create a loss report from a list of items.
     * The created report will later be inserted into storage using insert()
     *
     * @param items   the lost items we need a report for
     * @param message to provide some context
     * @return the loss report item
     */
    T createLossReport(List<T> items, String message);


    /**
     * Returns the max item id from a list of items
     *
     * @param items the batch
     * @return the max string id for this batch
     */
    String getMaxIdFromBatch(List<T> items);
}

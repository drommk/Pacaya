package com.drommk.pacaya;

import java.util.List;

import rx.Observable;

/**
 * Created by ericpalle on 12/3/15.
 */
public interface FlushService<T> {
    /**
     * Send a batch of {@link T} items to the remote storage
     *
     * @param events the batch of {@link T} events to send
     * @return the last eventId (base16) that's been received
     */
    Observable<String> sendBatch(List<T> events);


}

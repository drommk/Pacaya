package com.drommk.pacaya;

import com.google.gson.Gson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

/**
 * Created by ericpalle on 10/7/15.
 */
public class Pacaya<T> {
    private LocalStorageService<T> localStorageService;
    private FlushService<T> flushService;
    private ReportService<T> reportService;
    private final Integer batchMaxItems;
    private Subscription intervalSubscription;
    private Logger logger;
    private Long batchMaxSize;
    private Long flushMinSize;
    private Integer maxItemsInStorage;
    private PublishSubject<T> eventBus;

    private boolean isLocked = false;
    private final Subscriber<T> subscriber;
    private final Scheduler timeScheduler;

    Pacaya(Builder<T> builder) {
        this.localStorageService = builder.localStorageService;
        this.flushService = builder.flushService;
        this.reportService = builder.reportService;
        this.logger = builder.logger;
        this.batchMaxSize = builder.batchMaxUncompressedSize;
        this.batchMaxItems = builder.batchMaxItems;
        this.flushMinSize = builder.flusMinSize;
        this.maxItemsInStorage = builder.maxItemsInStorage;
        this.timeScheduler = builder.timeScheduler;

        eventBus = PublishSubject.create();
        this.subscriber = builder.subscriber;
        eventBus.buffer(1, TimeUnit.SECONDS, 10, timeScheduler)
                .filter(events -> events != null && !events.isEmpty())
                .doOnNext(events -> logger.debug("inserting {} items", events.size()))
                .flatMap(localStorageService::insert)
                .filter(event -> flushMinSize > 0)
                .throttleLast(60, TimeUnit.SECONDS, timeScheduler)
                .doOnNext(event -> checkAndSendBatch(false))
                .subscribe(subscriber);
    }

    public void startPeriodicFlush(long interval, TimeUnit unit) {
        intervalSubscription = Observable.interval(interval, unit, timeScheduler)
                .doOnNext(aLong -> logger.debug("-> STARTING PERIODIC FLUSH - {}", aLong))
                .doOnNext(aLong -> checkAndSendBatch(true))
                .subscribe(aLong -> logger.debug("-> ENDING PERIODIC FLUSH - {}", aLong)
                        , subscriber::onError);
    }

    public void stopPeriodicFlush() {
        if (intervalSubscription == null) {
            return;
        }
        intervalSubscription.unsubscribe();
    }

    public void log(T event) {
        eventBus.onNext(event);
    }

    void checkAndSendBatch(boolean isPeriodic) {
        if (isLocked) {
            logger.debug("already flushing, abort");
            return;
        }
        isLocked = true;

        localStorageService.count()
                .flatMap(this::autoPrune)
                .flatMap(nothing -> localStorageService.list(batchMaxItems))
                .filter(events2 -> events2 != null && events2.size() > 0)
                .doOnNext(events -> logger.info("tried to get {} items, found {}", batchMaxItems, events.size()))
                .flatMap(events -> checkBatchSize(events, isPeriodic))
                .filter(events1 -> events1 != null && events1.size() > 0)
                .doOnNext(events -> logger.info("flushing batch of {} events", events.size()))
                .flatMap(this::flush)
                .doOnNext(aLong -> logger.info("deleting up to {}", aLong))
                .flatMap(localStorageService::deleteUpTo)
                .finallyDo(() -> isLocked = false)
                .subscribe(intObservable -> logger.debug("checked"),
                        subscriber::onError);
    }

    private Observable<String> flush(List<T> events) {
        return flushService.sendBatch(events)
                .onErrorResumeNext(throwable -> {
                    if (reportService != null && throwable instanceof BatchRejectedException) {
                        return addLossReportForBatch(events, ReportService.BATCH_REJECTED);
                    }
                    return Observable.error(throwable);
                });
    }

    private Observable<? extends String> addLossReportForBatch(List<T> events, String reason) {
        T lostBatchItem = reportService.createLossReport(events, reason);

        return localStorageService.insert(Collections.singletonList(lostBatchItem))
                .map(t -> reportService.getMaxIdFromBatch(events));
    }

    private Observable<? extends Integer> autoPrune(Integer count) {
        logger.debug("local storage contains {} items ", count);
        if (count > maxItemsInStorage) {
            final int nbItemsToPrune = count - maxItemsInStorage + Double.valueOf(Math.ceil(maxItemsInStorage / 10)).intValue();
            logger.debug("autoprune : deleting the {} older items ", nbItemsToPrune);

            if (reportService != null) {
                localStorageService.list(nbItemsToPrune)
                        .flatMap(events -> this.addLossReportForBatch(events, ReportService.BUFFER_FULL))
                        .subscribe(x -> logger.debug("succesfully created loss report"),
                                subscriber::onError);
            }

            return localStorageService.deleteOlderItems(nbItemsToPrune);
        }
        return Observable.just(0);
    }

    private Observable<? extends List<T>> checkBatchSize(List<T> events, boolean ignoreMinimalFlushSizeCheck) {
        long batchSize = 0;
        Gson gson = new Gson();
        List<T> filteredEvents = new ArrayList<>();
        for (T event : events) {
            long newSize = batchSize + gson.toJson(event).getBytes().length;
            if (newSize > batchMaxSize) {
                logger.debug("reached max batch size");
                return Observable.just(filteredEvents);
            }
            batchSize = newSize;
            filteredEvents.add(event);
        }

        if (!ignoreMinimalFlushSizeCheck && (flushMinSize > 0 && batchSize < flushMinSize)) {
            logger.debug("autoflush min size has not been reached ({} < {}), waiting for more", batchSize, flushMinSize);
            return null;
        }
        return Observable.just(filteredEvents);
    }

    public static class Builder<T> {
        public static final Long DEFAULT_BATCH_MAX_SIZE_KB = 4 * 1000 * 1024L;
        public static final Long DEFAULT_AUTOMATIC_FLUSH_SIZE = -1L;
        public static final Integer DEFAULT_BATCH_MAX_ITEMS = 1000;
        public static final Integer DEFAULT_MAX_ITEMS_IN_STORAGE = 100 * 1000;

        LocalStorageService<T> localStorageService;
        FlushService<T> flushService;
        ReportService<T> reportService;
        Long flusMinSize;
        Long batchMaxUncompressedSize;
        Integer batchMaxItems;
        Integer maxItemsInStorage;
        Subscriber<T> subscriber;
        Scheduler timeScheduler;
        Logger logger;

        public Builder<T> logger(Logger logger) {
            this.logger = logger;
            return this;
        }

        public Builder<T> autoFlushSize(long automaticFlushSize) {
            this.flusMinSize = automaticFlushSize;
            return this;
        }

        public Builder<T> batchMaxUncompressedSize(long batchMaxSizeKb) {
            this.batchMaxUncompressedSize = batchMaxSizeKb;
            return this;
        }

        public Builder<T> batchMaxItems(int batchMaxItems) {
            this.batchMaxItems = batchMaxItems;
            return this;
        }

        public Builder<T> localStorageService(LocalStorageService<T> localStorageServiceService) {
            this.localStorageService = localStorageServiceService;
            return this;
        }

        public Builder<T> reportService(ReportService<T> reportService) {
            this.reportService = reportService;
            return this;
        }

        public Builder<T> flushService(FlushService<T> flushService) {
            this.flushService = flushService;
            return this;
        }

        public Builder<T> maxItemsInStorage(int maxItemsInStorage) {
            this.maxItemsInStorage = maxItemsInStorage;
            return this;
        }

        public Builder<T> subscriber(Subscriber<T> subscriber) {
            this.subscriber = subscriber;
            return this;
        }

        public Builder<T> timeScheduler(Scheduler timeScheduler) {
            this.timeScheduler = timeScheduler;
            return this;
        }

        public Pacaya<T> build() {
            String missingFields = "";
            if (flushService == null) {
                missingFields += " flushService";
            }

            if (localStorageService == null) {
                missingFields += " localStorageService";
            }

            if (!missingFields.isEmpty()) {
                throw new RuntimeException("Pacaya.Builder - missing :" + missingFields);
            }

            if (batchMaxUncompressedSize == null) {
                batchMaxUncompressedSize = DEFAULT_BATCH_MAX_SIZE_KB;
            }

            if (batchMaxItems == null) {
                batchMaxItems = DEFAULT_BATCH_MAX_ITEMS;
            }

            if (logger == null) {
                logger = LoggerFactory.getLogger("Pacaya");
            }

            if (flusMinSize == null) {
                flusMinSize = DEFAULT_AUTOMATIC_FLUSH_SIZE;
            }

            if (maxItemsInStorage == null) {
                maxItemsInStorage = DEFAULT_MAX_ITEMS_IN_STORAGE;
            }

            if (timeScheduler == null) {
                timeScheduler = Schedulers.computation();
            }

            if (subscriber == null) {
                subscriber = new Subscriber<T>() {
                    @Override
                    public void onCompleted() {
                        logger.debug("completed");
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        logger.error(throwable.getMessage());
                        throwable.printStackTrace();
                    }

                    @Override
                    public void onNext(Object o) {
                    }
                };
            }

            return new Pacaya<>(this);
        }
    }
}

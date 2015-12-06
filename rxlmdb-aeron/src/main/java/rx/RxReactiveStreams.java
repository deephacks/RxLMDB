package rx;

import org.reactivestreams.Publisher;
import rx.internal.reactivestreams.PublisherAdapter;
import rx.internal.reactivestreams.SubscriberAdapter;

/**
 * This type provides static factory methods for converting to and from RxJava types and Reactive Streams types.
 * <p/>
 * The <a href="http://www.reactive-streams.org">Reactive Streams</a> API provides a common API for interoperability
 * between different reactive streaming libraries, of which RxJava is one.
 * Using the methods of this class, RxJava can collaborate with other such libraries that also implement the standard.
 */
public abstract class RxReactiveStreams {

    private RxReactiveStreams() {
    }

    /**
     * Convert a Rx {@link Observable} into a Reactive Streams {@link Publisher}.
     * <p/>
     * Use this method when you have an RxJava observable, that you want to be consumed by another library.
     *
     * @param observable the {@link Observable} to convert
     * @return the converted {@link Publisher}
     */
    public static <T> Publisher<T> toPublisher(Observable<T> observable) {
        return new PublisherAdapter<T>(observable);
    }

    /**
     * Convert a Reactive Streams {@link Publisher} into a Rx {@link Observable}.
     * <p/>
     * Use this method when you have a stream from another library, that you want to be consume as an RxJava observable.
     *
     * @param publisher the {@link Publisher} to convert.
     * @return the converted {@link Observable}
     */
    public static <T> Observable<T> toObservable(final Publisher<T> publisher) {
        return Observable.create(new Observable.OnSubscribe<T>() {
            @Override
            public void call(final rx.Subscriber<? super T> rxSubscriber) {
                publisher.subscribe(toSubscriber(rxSubscriber));
            }
        });
    }

    /**
     * Convert an RxJava {@link rx.Subscriber} into a Reactive Streams {@link org.reactivestreams.Subscriber}.
     *
     * @param rxSubscriber an RxJava subscriber
     * @return a Reactive Streams subscriber
     */
    public static <T> org.reactivestreams.Subscriber<T> toSubscriber(final rx.Subscriber<T> rxSubscriber) {
        return new SubscriberAdapter<T>(rxSubscriber);
    }

}
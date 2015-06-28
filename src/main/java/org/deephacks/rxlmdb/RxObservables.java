/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.deephacks.rxlmdb;

import rx.Observable;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class RxObservables {
  static <T> Stream<T> toStreamBlocking(Observable<List<T>> observable) {
    Stream<List<T>> stream = StreamSupport.stream(observable.toBlocking().toIterable().spliterator(), false);
    Stream<T> reduce = stream.map(list -> list.spliterator())
      .map(split -> StreamSupport.stream(split, false))
      .reduce(Stream.empty(), (s1, s2) -> Stream.concat(s1, s2));
    return reduce;
  }
}

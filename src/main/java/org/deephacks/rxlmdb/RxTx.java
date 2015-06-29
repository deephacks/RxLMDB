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


import org.fusesource.lmdbjni.Transaction;

public class RxTx {
  final Transaction tx;
  final boolean isUserManaged;

  RxTx(Transaction tx, boolean isUserManaged) {
    this.tx = tx;
    this.isUserManaged = isUserManaged;
  }

  public void abort() {
    tx.abort();
  }

  public void commit() {
    tx.commit();
  }

  public void close() {
    tx.close();
  }

}

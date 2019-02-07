/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.github.ambry.account;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.json.JSONException;
import org.json.JSONObject;


public abstract class BaseAccountService implements AccountService {
  protected final AccountServiceMetrics accountServiceMetrics;
  protected final AtomicReference<AccountInfoMap> accountInfoMapRef = new AtomicReference<>(new AccountInfoMap());
  protected final CopyOnWriteArraySet<Consumer<Collection<Account>>> accountUpdateConsumers = new CopyOnWriteArraySet<>();

  public BaseAccountService(AccountServiceMetrics accountServiceMetrics) {
    this.accountServiceMetrics = Objects.requireNonNull(accountServiceMetrics, "accountServiceMetrics cannot be null");
  }

  @Override
  public Account getAccountByName(String accountName) {
    checkOpen();
    Objects.requireNonNull(accountName, "accountName cannot be null.");
    return accountInfoMapRef.get().getAccountByName(accountName);
  }

  @Override
  public Account getAccountById(short id) {
    checkOpen();
    return accountInfoMapRef.get().getAccountById(id);
  }

  @Override
  public Collection<Account> getAllAccounts() {
    checkOpen();
    return accountInfoMapRef.get().getAccounts();
  }

  @Override
  public boolean addAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
    checkOpen();
    Objects.requireNonNull(accountUpdateConsumer, "accountUpdateConsumer to subscribe cannot be null");
    return accountUpdateConsumers.add(accountUpdateConsumer);
  }

  @Override
  public boolean removeAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
    checkOpen();
    Objects.requireNonNull(accountUpdateConsumer, "accountUpdateConsumer to unsubscribe cannot be null");
    return accountUpdateConsumers.remove(accountUpdateConsumer);
  }


  /**
   * Checks if the account service is open.
   */
  protected void checkOpen() {
    if (isOpen()) {
      throw new IllegalStateException("AccountService is closed.");
    }
  }

  protected abstract boolean isOpen();

  /**
   * <p>
   *   A helper class that represents a collection of {@link Account}s, where the ids and names of the
   *   {@link Account}s are one-to-one mapped. An {@code AccountInfoMap} guarantees no duplicated account
   *   id or name, nor conflict among the {@link Account}s within it.
   * </p>
   * <p>
   *   Based on the properties, a {@code AccountInfoMap} internally builds index for {@link Account}s using both
   *   {@link Account}'s id and name as key.
   * </p>
   */
  protected class AccountInfoMap {
    private final Map<String, Account> nameToAccountMap;
    protected final Map<Short, Account> idToAccountMap;

    /**
     * Constructor for an empty {@code AccountInfoMap}.
     */
    private AccountInfoMap() {
      nameToAccountMap = new HashMap<>();
      idToAccountMap = new HashMap<>();
    }

    /**
     * <p>
     *   Constructs an {@code AccountInfoMap} from a group of {@link Account}s. The {@link Account}s exists
     *   in the form of a string-to-string map, where the key is the string form of an {@link Account}'s id,
     *   and the value is the string form of the {@link Account}'s JSON string.
     * </p>
     * <p>
     *   The source {@link Account}s in the {@code accountMap} may duplicate account ids or names, or corrupted
     *   JSON strings that cannot be parsed as valid {@link JSONObject}. In such cases, construction of
     *   {@code AccountInfoMap} will fail.
     * </p>
     * @param accountMap A map of {@link Account}s in the form of (accountIdString, accountJSONString).
     * @throws JSONException If parsing account data in json fails.
     */
    protected AccountInfoMap(Map<String, String> accountMap) throws JSONException {
      nameToAccountMap = new HashMap<>();
      idToAccountMap = new HashMap<>();
      for (Map.Entry<String, String> entry : accountMap.entrySet()) {
        String idKey = entry.getKey();
        String valueString = entry.getValue();
        Account account;
        JSONObject accountJson = new JSONObject(valueString);
        if (idKey == null) {
          accountServiceMetrics.remoteDataCorruptionErrorCount.inc();
          throw new IllegalStateException(
              "Invalid account record when reading accountMap in ZNRecord because idKey=null");
        }
        account = Account.fromJson(accountJson);
        if (account.getId() != Short.valueOf(idKey)) {
          accountServiceMetrics.remoteDataCorruptionErrorCount.inc();
          throw new IllegalStateException(
              "Invalid account record when reading accountMap in ZNRecord because idKey and accountId do not match. idKey="
                  + idKey + " accountId=" + account.getId());
        }
        if (idToAccountMap.containsKey(account.getId()) || nameToAccountMap.containsKey(account.getName())) {
          throw new IllegalStateException(
              "Duplicate account id or name exists. id=" + account.getId() + " name=" + account.getName());
        }
        idToAccountMap.put(account.getId(), account);
        nameToAccountMap.put(account.getName(), account);
      }
    }

    /**
     * Gets {@link Account} by its id.
     * @param id The id to get the {@link Account}.
     * @return The {@link Account} with the given id, or {@code null} if such an {@link Account} does not exist.
     */
    protected Account getAccountById(Short id) {
      return idToAccountMap.get(id);
    }

    /**
     * Gets {@link Account} by its name.
     * @param name The id to get the {@link Account}.
     * @return The {@link Account} with the given name, or {@code null} if such an {@link Account} does not exist.
     */
    protected Account getAccountByName(String name) {
      return nameToAccountMap.get(name);
    }

    /**
     * Checks if there is an {@link Account} with the given id.
     * @param id The {@link Account} id to check.
     * @return {@code true} if such an {@link Account} exists, {@code false} otherwise.
     */
    private boolean containsId(Short id) {
      return idToAccountMap.containsKey(id);
    }

    /**
     * Checks if there is an {@link Account} with the given name.
     * @param name The {@link Account} name to check.
     * @return {@code true} if such an {@link Account} exists, {@code false} otherwise.
     */
    private boolean containsName(String name) {
      return nameToAccountMap.containsKey(name);
    }

    /**
     * Gets all the {@link Account}s in this {@code AccountInfoMap} in a {@link Collection}.
     * @return A {@link Collection} of all the {@link Account}s in this map.
     */
    protected Collection<Account> getAccounts() {
      return Collections.unmodifiableCollection(idToAccountMap.values());
    }
  }
}

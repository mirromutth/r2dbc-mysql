/*
 * Copyright 2018-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.miku.r2dbc.mysql.message.server;

/**
 * Decode context with static creators.
 */
public interface DecodeContext {

    /**
     * Get an instance of {@link DecodeContext} for login phase.
     *
     * @return an instance for login phase.
     */
    static DecodeContext login() {
        return LoginDecodeContext.INSTANCE;
    }

    /**
     * Get an instance of {@link DecodeContext} for command phase.
     *
     * @return an instance for command phase.
     */
    static DecodeContext command() {
        return CommandDecodeContext.INSTANCE;
    }

    /**
     * Get an instance of {@link DecodeContext} after prepare statement query.
     *
     * @return an instance after prepare statement query.
     */
    static DecodeContext prepareQuery() {
        return PrepareQueryDecodeContext.INSTANCE;
    }

    /**
     * Get an instance of {@link DecodeContext} when fetching result for prepared statement executing.
     *
     * @return an instance for fetching.
     */
    static DecodeContext fetch() {
        return FetchDecodeContext.INSTANCE;
    }

    /**
     * Get an instance of {@link DecodeContext} when receive a result.
     *
     * @param eofDeprecated if EOF is deprecated.
     * @param totalColumns  the number of total column metadata.
     * @return an instance for receive result.
     */
    static DecodeContext result(boolean eofDeprecated, int totalColumns) {
        return new ResultDecodeContext(eofDeprecated, totalColumns);
    }

    /**
     * Get an instance of {@link DecodeContext} when receive metadata of prepared statement executing.
     *
     * @param eofDeprecated   if EOF is deprecated.
     * @param totalColumns    the number of total column metadata.
     * @param totalParameters the number of total parameters.
     * @return an instance for receive prepared metadata.
     */
    static DecodeContext preparedMetadata(boolean eofDeprecated, int totalColumns, int totalParameters) {
        return new PreparedMetadataDecodeContext(eofDeprecated, totalColumns, totalParameters);
    }
}

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import io.airlift.units.DataSize;

public class ExceededMemoryLimitException
        extends PrestoException
{
    public ExceededMemoryLimitException(DataSize maxMemory)
    {
        this(maxMemory, "Task");
    }

    public ExceededMemoryLimitException(DataSize maxMemory, String limitName)
    {
        super(StandardErrorCode.EXCEEDED_MEMORY_LIMIT.toErrorCode(), String.format("%s exceeded max memory size of %s", limitName, maxMemory));
    }
}

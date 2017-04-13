/*
 * Copyright 2017 the original author or authors.
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
package com.lambdaworks.redis.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.CommandWrapper;

/**
 * @author Mark Paluch
 */
public class CommandWrapperTest {

    protected RedisCodec<String, String> codec = StringCodec.UTF8;
    protected Command<String, String, String> sut;

    @Before
    public final void createCommand() throws Exception {

        CommandOutput<String, String, String> output = new StatusOutput<>(codec);
        sut = new Command<>(CommandType.INFO, output, null);
    }

    @Test
    public void shouldAppendOnComplete() {

        AtomicReference<Boolean> v1 = new AtomicReference<>();
        AtomicReference<Boolean> v2 = new AtomicReference<>();

        CommandWrapper<String, String, String> commandWrapper = new CommandWrapper<>(sut);

        commandWrapper.onComplete(s -> v1.set(true));
        commandWrapper.onComplete(s -> v2.set(true));

        commandWrapper.complete();

        assertThat(v1.get()).isEqualTo(true);
        assertThat(v2.get()).isEqualTo(true);
    }
}

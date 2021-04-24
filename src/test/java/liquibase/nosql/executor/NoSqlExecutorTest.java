package liquibase.nosql.executor;

/*-
 * #%L
 * Liquibase MongoDB Extension
 * %%
 * Copyright (C) 2021 Mastercard
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import liquibase.Scope;
import liquibase.database.core.PostgresDatabase;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.executor.jvm.JdbcExecutor;
import liquibase.ext.mongodb.database.MongoLiquibaseDatabase;
import org.junit.jupiter.api.Test;

import static liquibase.nosql.executor.NoSqlExecutor.EXECUTOR_NAME;
import static org.assertj.core.api.Assertions.assertThat;

class NoSqlExecutorTest {

    @Test
    void testGetInstance() {
        final PostgresDatabase postgresDatabase = new PostgresDatabase();
        Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor(EXECUTOR_NAME, postgresDatabase);

        assertThat(executor).isNotNull()
                .isInstanceOfAny(JdbcExecutor.class);

        final MongoLiquibaseDatabase mongoDatabase = new MongoLiquibaseDatabase();
        executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor(EXECUTOR_NAME, mongoDatabase);

        assertThat(executor).isNotNull()
                .isInstanceOfAny(NoSqlExecutor.class);
    }

    @Test
    void setDatabase() {
    }

    @Test
    void queryForObject() {
    }

    @Test
    void queryForObject1() {
    }

    @Test
    void queryForLong() {
    }

    @Test
    void queryForInt() {
    }

    @Test
    void queryForList() {
    }

    @Test
    void execute() {
    }

    @Test
    void update() {
    }

    @Test
    void comment() {
    }

    @Test
    void updatesDatabase() {
    }

    @Test
    void setDb() {
    }

    @Test
    void getDb() {
    }
}

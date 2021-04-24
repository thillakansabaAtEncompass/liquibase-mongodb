package liquibase.ext;

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

import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.database.DatabaseFactory;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled("Manual enable when required to test collocated logic")
public class JdbcLiquibaseIT {

    private Database database;
    //private DatabaseConnection connection;

    @SneakyThrows
    @Test
    @BeforeEach
    protected void setUpEach() {
        database = DatabaseFactory.getInstance().openDatabase("jdbc:postgresql://localhost:5432/test_db", "test_user", "test_user", null, null);
        //connection = database.getConnection();
    }

    @Test
    void testLiquibase() throws LiquibaseException {
        Liquibase liquiBase = new Liquibase("test.sql", new ClassLoaderResourceAccessor(), database);
        liquiBase.update("");
    }

}

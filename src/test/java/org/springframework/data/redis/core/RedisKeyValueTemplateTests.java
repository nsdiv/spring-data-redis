/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.redis.core;

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsCollectionContaining.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 */
@RunWith(Parameterized.class)
public class RedisKeyValueTemplateTests {

	RedisConnectionFactory connectionFactory;
	RedisKeyValueTemplate template;
	RedisTemplate<Object, Object> nativeTemplate;

	public RedisKeyValueTemplateTests(RedisConnectionFactory connectionFactory) {

		this.connectionFactory = connectionFactory;
		ConnectionFactoryTracker.add(connectionFactory);
	}

	@Parameters
	public static List<RedisConnectionFactory> params() {

		JedisConnectionFactory jedis = new JedisConnectionFactory();
		jedis.afterPropertiesSet();

		LettuceConnectionFactory lettuce = new LettuceConnectionFactory();
		lettuce.afterPropertiesSet();

		return Arrays.<RedisConnectionFactory> asList(jedis, lettuce);
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	@Before
	public void setUp() {

		nativeTemplate = new RedisTemplate<Object, Object>();
		nativeTemplate.setConnectionFactory(connectionFactory);
		nativeTemplate.afterPropertiesSet();

		RedisMappingContext context = new RedisMappingContext();

		RedisKeyValueAdapter adapter = new RedisKeyValueAdapter(nativeTemplate, context);
		template = new RedisKeyValueTemplate(adapter, context);
	}

	@After
	public void tearDown() {

		nativeTemplate.execute(new RedisCallback<Void>() {

			@Override
			public Void doInRedis(RedisConnection connection) throws DataAccessException {

				connection.flushDb();
				return null;
			}
		});
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void savesObjectCorrectly() {

		final Person rand = new Person();
		rand.firstname = "rand";

		template.insert(rand);

		nativeTemplate.execute(new RedisCallback<Void>() {

			@Override
			public Void doInRedis(RedisConnection connection) throws DataAccessException {

				assertThat(connection.exists(("template-test-person:" + rand.id).getBytes()), is(true));
				return null;
			}
		});
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void findProcessesCallbackReturningSingleIdCorrectly() {

		Person rand = new Person();
		rand.firstname = "rand";

		final Person mat = new Person();
		mat.firstname = "mat";

		template.insert(rand);
		template.insert(mat);

		List<Person> result = template.find(new RedisCallback<byte[]>() {

			@Override
			public byte[] doInRedis(RedisConnection connection) throws DataAccessException {
				return mat.id.getBytes();
			}
		}, Person.class);

		assertThat(result.size(), is(1));
		assertThat(result, hasItems(mat));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void findProcessesCallbackReturningMultipleIdsCorrectly() {

		final Person rand = new Person();
		rand.firstname = "rand";

		final Person mat = new Person();
		mat.firstname = "mat";

		template.insert(rand);
		template.insert(mat);

		List<Person> result = template.find(new RedisCallback<List<byte[]>>() {

			@Override
			public List<byte[]> doInRedis(RedisConnection connection) throws DataAccessException {
				return Arrays.asList(rand.id.getBytes(), mat.id.getBytes());
			}
		}, Person.class);

		assertThat(result.size(), is(2));
		assertThat(result, hasItems(rand, mat));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void findProcessesCallbackReturningNullCorrectly() {

		Person rand = new Person();
		rand.firstname = "rand";

		Person mat = new Person();
		mat.firstname = "mat";

		template.insert(rand);
		template.insert(mat);

		List<Person> result = template.find(new RedisCallback<List<byte[]>>() {

			@Override
			public List<byte[]> doInRedis(RedisConnection connection) throws DataAccessException {
				return null;
			}
		}, Person.class);

		assertThat(result.size(), is(0));
	}

	@Test
	public void partialUpdate() {

		final Person rand = new Person();
		rand.firstname = "rand";

		template.insert(rand);

		PartialUpdate<Person> update = new PartialUpdate<Person>(rand.id, Person.class);

		/*
		 * Set the lastname and make sure we've an index on it afterwards
		 */
		Person update1 = new Person(rand.id, "Rand-Update", "al-thor");
		update.setValue(update1);

		assertThat(template.doPartialUpdate(update), is(update1));

		nativeTemplate.execute(new RedisCallback<Void>() {

			@Override
			public Void doInRedis(RedisConnection connection) throws DataAccessException {

				assertThat(connection.exists("template-test-person:lastname:al-thor".getBytes()), is(true));
				assertThat(connection.sIsMember("template-test-person:lastname:al-thor".getBytes(), rand.id.getBytes()),
						is(true));
				return null;
			}
		});

		/*
		 * Set the firstname and make sure lastname index and value is not affected
		 */
		update = new PartialUpdate<Person>(rand.id, Person.class);
		update.set("firstname", "frodo");

		assertThat(template.doPartialUpdate(update), is(new Person(rand.id, "frodo", "al-thor")));

		nativeTemplate.execute(new RedisCallback<Void>() {

			@Override
			public Void doInRedis(RedisConnection connection) throws DataAccessException {

				assertThat(connection.exists("template-test-person:lastname:al-thor".getBytes()), is(true));
				assertThat(connection.sIsMember("template-test-person:lastname:al-thor".getBytes(), rand.id.getBytes()),
						is(true));
				return null;
			}
		});

		/*
		 * Remote firstname and update lastname. Make sure lastname index is updated
		 */
		update = new PartialUpdate<Person>(rand.id, Person.class);
		update.del("firstname");
		update.set("lastname", "buggins");

		assertThat(template.doPartialUpdate(update), is(new Person(rand.id, null, "buggins")));

		nativeTemplate.execute(new RedisCallback<Void>() {

			@Override
			public Void doInRedis(RedisConnection connection) throws DataAccessException {

				assertThat(connection.exists("template-test-person:lastname:al-thor".getBytes()), is(false));
				assertThat(connection.exists("template-test-person:lastname:buggins".getBytes()), is(true));
				assertThat(connection.sIsMember("template-test-person:lastname:buggins".getBytes(), rand.id.getBytes()),
						is(true));
				return null;
			}
		});

		/*
		 * Remove lastname and make sure the index vanishes 
		 */
		update = new PartialUpdate<Person>(rand.id, Person.class);
		update.del("lastname");

		assertThat(template.doPartialUpdate(update), is(new Person(rand.id, null, null)));

		nativeTemplate.execute(new RedisCallback<Void>() {

			@Override
			public Void doInRedis(RedisConnection connection) throws DataAccessException {

				assertThat(connection.keys("template-test-person:lastname:*".getBytes()).size(), is(0));
				return null;
			}
		});

	}

	@RedisHash("template-test-person")
	static class Person {

		@Id String id;
		String firstname;
		@Indexed String lastname;
		Integer age;

		public Person() {}

		public Person(String firstname, String lastname) {
			this(null, firstname, lastname, null);
		}

		public Person(String id, String firstname, String lastname) {
			this(id, firstname, lastname, null);
		}

		public Person(String id, String firstname, String lastname, Integer age) {

			this.id = id;
			this.firstname = firstname;
			this.lastname = lastname;
			this.age = age;
		}

		@Override
		public int hashCode() {

			int result = ObjectUtils.nullSafeHashCode(firstname);
			result += ObjectUtils.nullSafeHashCode(lastname);
			result += ObjectUtils.nullSafeHashCode(age);
			return result + ObjectUtils.nullSafeHashCode(id);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (!(obj instanceof Person)) {
				return false;
			}
			Person that = (Person) obj;

			if (!ObjectUtils.nullSafeEquals(this.firstname, that.firstname)) {
				return false;
			}

			if (!ObjectUtils.nullSafeEquals(this.lastname, that.lastname)) {
				return false;
			}

			if (!ObjectUtils.nullSafeEquals(this.age, that.age)) {
				return false;
			}

			return ObjectUtils.nullSafeEquals(this.id, that.id);
		}

		@Override
		public String toString() {
			return "Person [id=" + id + ", firstname=" + firstname + ", lastname=" + lastname + ", age=" + age + "]";
		}

	}
}

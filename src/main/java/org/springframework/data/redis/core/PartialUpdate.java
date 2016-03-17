/*
 * Copyright 2016 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * @author Christoph Strobl
 * @param <T>
 */
public class PartialUpdate<T> {

	private final Object id;
	private final Class<T> target;
	private final T value;
	private boolean refreshTtl = false;

	private final List<PropertyUpdate> propertyUpdates = new ArrayList<PropertyUpdate>();

	private PartialUpdate(Object id, Class<T> target, T value, boolean refreshTtl, List<PropertyUpdate> propertyUpdates) {

		this.id = id;
		this.target = target;
		this.value = value;
		this.refreshTtl = refreshTtl;
		this.propertyUpdates.addAll(propertyUpdates);
	}

	/**
	 * @param id must not be {@literal null}.
	 * @param target must not be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	public PartialUpdate(Object id, Class<T> target) {

		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(target, "Target must not be null!");

		this.id = id;
		this.target = (Class<T>) ClassUtils.getUserClass(target);
		this.value = null;
	}

	/**
	 * @param id must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	public PartialUpdate(Object id, T value) {

		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(value, "Value must not be null!");

		this.id = id;
		this.target = (Class<T>) ClassUtils.getUserClass(value.getClass());
		this.value = value;
	}

	/**
	 * @return can be {@literal null}.
	 */
	public T getValue() {
		return value;
	}

	public PartialUpdate<T> set(String path, Object value) {

		propertyUpdates.add(new PropertyUpdate(UpdateCommand.SET, path, value));
		return new PartialUpdate<T>(this.id, this.target, this.value, this.refreshTtl, this.propertyUpdates);
	}

	public PartialUpdate<T> del(String path) {

		propertyUpdates.add(new PropertyUpdate(UpdateCommand.DEL, path));
		return new PartialUpdate<T>(this.id, this.target, this.value, this.refreshTtl, this.propertyUpdates);
	}

	/**
	 * @return never {@literal null}.
	 */
	public Class<T> getTarget() {
		return target;
	}

	/**
	 * @return never {@literal null}.
	 */
	public Object getId() {
		return id;
	}

	public List<PropertyUpdate> getPropertyUpdates() {
		return Collections.unmodifiableList(propertyUpdates);
	}

	public boolean isRefreshTtl() {
		return refreshTtl;
	}

	public PartialUpdate<T> refreshTtl(boolean refreshTtl) {

		this.refreshTtl = refreshTtl;
		return new PartialUpdate<T>(this.id, this.target, this.value, this.refreshTtl, this.propertyUpdates);
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class PropertyUpdate {

		private final UpdateCommand cmd;
		private final String propertyPath;
		private final Object value;

		private PropertyUpdate(UpdateCommand cmd, String propertyPath) {
			this(cmd, propertyPath, null);
		}

		private PropertyUpdate(UpdateCommand cmd, String propertyPath, Object value) {

			this.cmd = cmd;
			this.propertyPath = propertyPath;
			this.value = value;
		}

		public UpdateCommand getCmd() {
			return cmd;
		}

		public String getPropertyPath() {
			return propertyPath;
		}

		public Object getValue() {
			return value;
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	public static enum UpdateCommand {
		SET, DEL
	}

}

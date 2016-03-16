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

/**
 * @author Christoph Strobl
 * @param <T>
 */
public class PartialUpdate<T> {

	private final Object id;
	private final Class<T> target;
	private T value;
	private boolean refreshTtl = false;

	private final List<PropertyUpdate> propertyUpdates = new ArrayList<PropertyUpdate>();

	public PartialUpdate(Object id, Class<T> type) {

		this.id = id;
		this.target = type;
	}

	public PartialUpdate(Object id, T value) {

		this.id = id;
		this.target = (Class<T>) value.getClass();
		this.value = value;
	}

	public void setValue(T value) {
		this.value = value;
	}

	public T getValue() {
		return value;
	}

	public void set(String path, Object value) {
		propertyUpdates.add(new PropertyUpdate(UpdateCommand.SET, path, value));
	}

	public void del(String path) {
		propertyUpdates.add(new PropertyUpdate(UpdateCommand.DEL, path, null));
	}

	public Class<T> getTarget() {
		return target;
	}

	public Object getId() {
		return id;
	}

	public List<PropertyUpdate> getPropertyUpdates() {
		return Collections.unmodifiableList(propertyUpdates);
	}

	public boolean isRefreshTtl() {
		return refreshTtl;
	}

	public void setRefreshTtl(boolean refreshTtl) {
		this.refreshTtl = refreshTtl;
	}

	static enum UpdateCommand {
		SET, DEL
	}

	static class PropertyUpdate {

		private final UpdateCommand cmd;
		private final String propertyPath;
		private final Object value;

		public PropertyUpdate(UpdateCommand cmd, String propertyPath, Object value) {

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

}

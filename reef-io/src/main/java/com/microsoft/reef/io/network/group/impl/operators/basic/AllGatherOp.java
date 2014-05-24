/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.io.network.group.impl.operators.basic;

import java.util.List;

import javax.inject.Inject;

import com.microsoft.reef.exception.evaluator.NetworkException;
import com.microsoft.reef.io.network.group.impl.GroupCommNetworkHandler;
import com.microsoft.reef.io.network.group.impl.operators.basic.config.GroupParameters;
import com.microsoft.reef.io.network.group.operators.AllGather;
import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Gather;
import com.microsoft.reef.io.network.impl.NetworkService;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.reef.io.network.util.ListCodec;
import com.microsoft.reef.io.network.util.Utils;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.ComparableIdentifier;
import com.microsoft.wake.Identifier;
import com.microsoft.wake.IdentifierFactory;
import com.microsoft.wake.remote.Codec;

/**
 * Implementation of {@link AllGather}
 *
 * @author shravan
 *
 * @param <T>
 */
public class AllGatherOp<T> extends SenderReceiverBase implements AllGather<T> {
	Gather.Sender<T> gatherSender;
	Gather.Receiver<T> gatherReceiver;
	Broadcast.Sender<List<T>> broadcastSender;
	Broadcast.Receiver<List<T>> broadcastReceiver;

	@Inject
	public AllGatherOp(
			final NetworkService<GroupCommMessage> netService,
			final GroupCommNetworkHandler multiHandler,
			@Parameter(GroupParameters.AllGather.DataCodec.class) final Codec<T> codec,
			@Parameter(GroupParameters.AllGather.SelfId.class) final String self,
			@Parameter(GroupParameters.AllGather.ParentId.class) final String parent,
			@Parameter(GroupParameters.AllGather.ChildIds.class) final String children,
			@Parameter(GroupParameters.IDFactory.class) final IdentifierFactory idFac){
		this(
				new GatherOp.Sender<>(netService, multiHandler, codec, self, parent, children, idFac),
				new GatherOp.Receiver<>(netService, multiHandler, codec, self, parent, children, idFac),
				new BroadcastOp.Sender<>(netService, multiHandler, new ListCodec<>(codec), self, parent, children, idFac),
				new BroadcastOp.Receiver<>(netService, multiHandler, new ListCodec<>(codec), self, parent, children, idFac),
				idFac.getNewInstance(self),
				(parent.equals(GroupParameters.defaultValue)) ? null : idFac.getNewInstance(parent),
				(children.equals(GroupParameters.defaultValue)) ? null : Utils.parseListCmp(children, idFac)
			);
	}

	public AllGatherOp(final Gather.Sender<T> gatherSender,
			final Gather.Receiver<T> gatherReceiver,
			final Broadcast.Sender<List<T>> broadcastSender,
			final Broadcast.Receiver<List<T>> broadcastReceiver, final Identifier self,
			final Identifier parent, final List<ComparableIdentifier> children) {
		super(self,parent,children);
		this.gatherSender = gatherSender;
		this.gatherReceiver = gatherReceiver;
		this.broadcastSender = broadcastSender;
		this.broadcastReceiver = broadcastReceiver;
	}

	@Override
	public List<T> apply(final T element) throws NetworkException,
			InterruptedException {
		return apply(element,getChildren());
	}

	@Override
	public List<T> apply(final T element, final List<? extends Identifier> order)
			throws NetworkException, InterruptedException {
		List<T> result = null;
		if (getParent() == null) {
			result = gatherReceiver.receive(order);
			broadcastSender.send(result);
		} else {
			gatherSender.send(element);
			result = broadcastReceiver.receive();
		}
		return result;
	}

}

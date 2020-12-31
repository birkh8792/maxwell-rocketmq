package com.zendesk.maxwell.rocketmq.producerfactory;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.row.RowMap;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Custom {@link AbstractProducer} example that collects all the rows for a transaction and writes them to standard out.
 * To ensure atomicity, the producer saves its position in the binlog only after writing a full transaction to stdout.
 *
 * When writing a producer, bear in mind the following ordering guarantees provided by maxwell and the binlog (in row 
 * mode, with no non-transactional tables involved). For every call to push(RowMap) in your producer, the following is 
 * guaranteed:
 *
 *  - The binlog position is monotonically (but not strictly) increasing.
 *  - Transaction IDs are not guaranteed to be monotonically increasing.
 *  - Each transaction is stored in one chunk and delivered in order - no interleaving of transactions ever occur.
 *  - Rolled back transactions are not stored on the binlog and hence never delivered.
 */
public class RocketmqProducer extends AbstractProducer {
	private static final Logger LOGGER = LoggerFactory.getLogger(RocketmqProducer.class);
	// 实例化消息生产者Producer
	private DefaultMQProducer producer;

	private String rocketmqSendTopic;
	private String rocketmqTags;


	public RocketmqProducer(MaxwellContext context) {
		super(context);
		try {
			// this property would be 'custom_producer.header_format' in config.properties
			//headerFormat = context.getConfig().customProducerProperties.getProperty("header_format", "Transaction: %xid% >>>\n");

			producer = new DefaultMQProducer(context.getConfig().customProducerProperties.getProperty("rocketmq_producer_group", "rocketmq_producer_group"));
			// 设置NameServer的地址
			producer.setNamesrvAddr(context.getConfig().customProducerProperties.getProperty("rocketmq_namesrv_addr", "localhost:9876"));
			rocketmqSendTopic = context.getConfig().customProducerProperties.getProperty("rocketmq_send_topic", "maxwell");
			rocketmqTags = context.getConfig().customProducerProperties.getProperty("rocketmq_tags", "*");

			// 启动Producer实例
			producer.start();
		} catch (MQClientException e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public void push(RowMap r) throws Exception {
		if ( !r.shouldOutput(outputConfig) ) {
			context.setPosition(r.getNextPosition());
			return;
		}
		String value = r.toJSON(outputConfig);
		// 创建消息，并指定Topic，Tag和消息体
		Message msg = new Message(rocketmqSendTopic /* Topic */,
				rocketmqTags /* Tag */,
				(value).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
		);

		producer.send(msg);

		if ( r.isTXCommit() ) {
			context.setPosition(r.getNextPosition());
		}
		if ( LOGGER.isDebugEnabled()) {
			LOGGER.debug("->  topic:" + rocketmqSendTopic + ", tags:" + rocketmqTags);
		}
	}
}

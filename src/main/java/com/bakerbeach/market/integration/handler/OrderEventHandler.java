package com.bakerbeach.market.integration.handler;

import java.util.HashMap;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bakerbeach.market.com.api.ComConnector;
import com.bakerbeach.market.com.api.ComConnectorException;
import com.bakerbeach.market.com.api.DataMapKeys;
import com.bakerbeach.market.com.api.MessageType;
import com.bakerbeach.market.core.api.model.Customer;
import com.bakerbeach.market.core.api.model.Order;
import com.bakerbeach.market.customer.api.service.CustomerService;
import com.bakerbeach.market.customer.api.service.CustomerServiceException;
import com.bakerbeach.market.order.api.service.OrderService;
import com.bakerbeach.market.order.api.service.OrderServiceException;


public class OrderEventHandler extends AbstractHandler {
	final Logger log = LoggerFactory.getLogger(OrderEventHandler.class);

	private ComConnector comConnector;

	private OrderService orderService;

	private CustomerService customerService;

	public void order(Exchange ex) {
		try {
			@SuppressWarnings("unchecked")
			Map<String, Object> payload = (Map<String, Object>) getPayload(ex.getIn());
			Map<String, Object> data = new HashMap<String, Object>();
			if (payload.containsKey("shop_code")) {
				data.put(DataMapKeys.SHOP_CODE, (String) payload.get("shop_code"));
			} else {
				throw new EventHandlerException("missing shop_code parameter for order mail");
			}
			if (payload.containsKey("order_id")) {
				Order order = orderService.findOrderById((String) payload.get("order_id"));
				if (order != null) {
					data.put(DataMapKeys.ORDER, order);
					Customer customer = customerService.findById(order.getCustomerId());
					data.put(DataMapKeys.CUSTOMER, customer);
				}else{
					throw new EventHandlerException("order not found with id " + (String) payload.get("order_id") + " for order mail");
				}
			}else{
				throw new EventHandlerException("missing order_id parameter for order mail");
			}
			comConnector.generateMessageAndSend(MessageType.ORDER, data);
		} catch (EventHandlerException | ComConnectorException | CustomerServiceException | OrderServiceException e) {
			log.error(ExceptionUtils.getFullStackTrace(e));
		}
	}

	public void cancel(Exchange ex) {
		try {
			@SuppressWarnings("unchecked")
			Map<String, Object> payload = (Map<String, Object>) getPayload(ex.getIn());
			Map<String, Object> data = new HashMap<String, Object>();
			if (payload.containsKey("shop_code")) {
				data.put(DataMapKeys.SHOP_CODE, payload.containsKey("shop_code"));
			} else {
				throw new EventHandlerException("missing shop_code parameter for cancel mail");
			}
			if (payload.containsKey("order_id")) {
				Order order = orderService.findOrderById((String) payload.get("orderId"));
				if (order != null) {
					data.put(DataMapKeys.ORDER, order);
					Customer customer = customerService.findById(order.getCustomerId());
					data.put(DataMapKeys.CUSTOMER, customer);
				}else{
					throw new EventHandlerException("order not found with id " + (String) payload.get("order_id") + " for cancel mail");
				}
			}else{
				throw new EventHandlerException("missing order_id parameter for cancel mail");
			}
			comConnector.generateMessageAndSend(MessageType.CANCELD, data);
		} catch (EventHandlerException | ComConnectorException | CustomerServiceException | OrderServiceException e) {
			log.error(ExceptionUtils.getFullStackTrace(e));
		}
	}

	public void warning(Exchange ex) {
		try {
			@SuppressWarnings("unchecked")
			Map<String, Object> payload = (Map<String, Object>) getPayload(ex.getIn());
			Map<String, Object> data = new HashMap<String, Object>();
			if (payload.containsKey("shop_code")) {
				data.put(DataMapKeys.SHOP_CODE, payload.containsKey("shop_code"));
			} else {
				throw new EventHandlerException("missing shop_code parameter for payment warning mail");
			}
			if (payload.containsKey("order_id")) {
				Order order = orderService.findOrderById((String) payload.get("orderId"));
				if (order != null) {
					data.put(DataMapKeys.ORDER, order);
					Customer customer = customerService.findById(order.getCustomerId());
					data.put(DataMapKeys.CUSTOMER, customer);
				}else{
					throw new EventHandlerException("order not found with id " + (String) payload.get("order_id") + " for payment warning mail");
				}
			}else{
				throw new EventHandlerException("missing order_id parameter for payment warning mail");
			}
			comConnector.generateMessageAndSend(MessageType.PAYMENT_WARNING, data);
		} catch (EventHandlerException | ComConnectorException | CustomerServiceException | OrderServiceException e) {
			log.error(ExceptionUtils.getFullStackTrace(e));
		}
	}

	public void payment(Exchange ex) {
		try {
			@SuppressWarnings("unchecked")
			Map<String, Object> payload = (Map<String, Object>) getPayload(ex.getIn());
			Map<String, Object> data = new HashMap<String, Object>();
			if (payload.containsKey("shop_code")) {
				data.put(DataMapKeys.SHOP_CODE, payload.containsKey("shop_code"));
			} else {
				throw new EventHandlerException("missing shop_code parameter for payment mail");
			}
			if (payload.containsKey("order_id")) {
				Order order = orderService.findOrderById((String) payload.get("orderId"));
				if (order != null) {
					data.put(DataMapKeys.ORDER, order);
					Customer customer = customerService.findById(order.getCustomerId());
					data.put(DataMapKeys.CUSTOMER, customer);
				}else{
					throw new EventHandlerException("order not found with id " + (String) payload.get("order_id") + " for payment mail");
				}
			}else{
				throw new EventHandlerException("missing order_id parameter for payment mail");
			}
			comConnector.generateMessageAndSend(MessageType.PAYMENT, data);
		} catch (EventHandlerException | ComConnectorException | CustomerServiceException | OrderServiceException e) {
			log.error(ExceptionUtils.getFullStackTrace(e));
		}
	}

	public void deliveryNote(Exchange ex) {

	}

	public ComConnector getComConnector() {
		return comConnector;
	}

	public void setComConnector(ComConnector comConnector) {
		this.comConnector = comConnector;
	}

	public OrderService getOrderService() {
		return orderService;
	}

	public void setOrderService(OrderService orderService) {
		this.orderService = orderService;
	}

	public CustomerService getCustomerService() {
		return customerService;
	}

	public void setCustomerService(CustomerService customerService) {
		this.customerService = customerService;
	}



}

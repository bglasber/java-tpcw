package servlets;

public abstract class DMConstants {
	// scale is NUM_CUSTOMERS * 2 = 5760 * num_eb
	public static final int ADDRESS_TABLE_ID = 1;

	// scale is 0.25 * num_eb
	public static final int AUTHOR_TABLE_ID = 2;

	// scale is 92
	public static final int COUNTRY_TABLE_ID = 3;

	// scale is 2880 * num_eb
	public static final int CUSTOMER_TABLE_ID = 4;

	// scale is num_item
	public static final int ITEM_TABLE_ID = 5;

	// scale is NUM_CUSTOMERS * 0.9 = 2552 * num_eb
	public static final int ORDER_TABLE_ID = 6;
	public static final int CC_XACTS_TABLE_ID = 7;


	// scale is 5 * NUM_ORDERS = 12760 * num_eb
	public static final int ORDER_LINE_TABLE_ID = 8;

	// scale is unknown
	public static final int SHOPPING_CART_TABLE_ID = 9;

	// scale is unknown (assume same as shopping cart)
	public static final int SHOPPING_CART_LINE_TABLE_ID = 10;

	// are you adding new things? Better update DMUtil to handle this



}

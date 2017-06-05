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
}

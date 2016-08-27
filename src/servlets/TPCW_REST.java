package servlets;

import java.sql.SQLException;
import java.util.Vector;
import java.util.Date;
import java.util.Enumeration;
import java.text.SimpleDateFormat;


import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONException;

import com.sun.jersey.api.client.WebResource.Builder;
import common.SQL;

public class TPCW_REST {

    private static final Builder builder = RESTUtil.makeRestConnection(1);

    public static String[] getName(int cid){
        String name[] = new String[2];
        String stmt = SQL.getName;
        try {
            JSONArray rs = RESTUtil.executeSelectQuery(builder, stmt, String.valueOf(cid));
            name[0] = rs.getJSONObject(0).getString("c_fname");
            name[1] = rs.getJSONObject(0).getString("c_lname");
        } catch( SQLException | JSONException e ){
            e.printStackTrace();
        }
        return name;
    }

    public static Book getBook(int i_id){
        Book book = null;
        String stmt = SQL.getBook;
        try {
            JSONArray rs = RESTUtil.executeSelectQuery(builder, stmt, String.valueOf(i_id));
            book = new Book(rs.getJSONObject(0));
        } catch( SQLException | JSONException e ) {
            e.printStackTrace();
        }

        return book;
    }

    public static Customer getCustomer(String uname){
        Customer cust = null;
        String stmt = SQL.getCustomer;
        try {
            JSONArray rs = RESTUtil.executeSelectQuery(builder, stmt, "'" + uname + "'");
            cust = new Customer(rs.getJSONObject(0));
        } catch( SQLException | JSONException e ){
            e.printStackTrace();
        }
        return cust;
    }

    public static Vector doSubjectSearch(String searchKey){
        Vector vec = new Vector();
        try {
            String stmt = SQL.doSubjectSearch;
            JSONArray rs = RESTUtil.executeSelectQuery(builder, stmt, "'" + searchKey + "'" );
            for(int i = 0; i < rs.length(); i++){
                Book b = new Book(rs.getJSONObject(i));
                vec.addElement(b);
            }
        } catch( SQLException | JSONException e ){
            e.printStackTrace();
        }
        return vec;
    }

    public static Vector doTitleSearch(String searchKey){
        Vector vec = new Vector();
        try {
            String stmt = SQL.doTitleSearch;
            JSONArray rs = RESTUtil.executeSelectQuery(builder, stmt, "'" + searchKey + "'");
            for(int i = 0; i < rs.length(); i++){
                Book b = new Book(rs.getJSONObject(i));
                vec.addElement(b);
            }
        } catch( SQLException | JSONException e ){
            e.printStackTrace();
        }
        return vec;
    }

    public static Vector doAuthorSearch(String searchKey){
        Vector vec = new Vector();
        try {
            String stmt = SQL.doAuthorSearch;
            JSONArray rs = RESTUtil.executeSelectQuery(builder, stmt, "'" + searchKey + "'");
            for(int i = 0; i < rs.length(); i++){
                Book b = new Book(rs.getJSONObject(i));
                vec.addElement(b);
            }
        } catch( SQLException | JSONException e ){
            e.printStackTrace();
        }
        return vec;
    }

    public static Vector getNewProducts(String subject){
        Vector vec = new Vector();
        try {
            String stmt = SQL.getNewProducts;
            JSONArray rs = RESTUtil.executeSelectQuery(builder, stmt, "'" + subject + "'");
            for(int i = 0; i < rs.length(); i++){
                ShortBook b = new ShortBook(rs.getJSONObject(i));
                vec.addElement(b);
            }
        } catch( SQLException | JSONException e ){
            e.printStackTrace();
        }
        return vec;
    }

    public static Vector getBestSellers(String subject){
        Vector vec = new Vector();
        try {
            String stmt = SQL.getBestSellers;
            JSONArray rs = RESTUtil.executeSelectQuery(builder, stmt, "'" + subject + "'");
            for(int i = 0; i < rs.length(); i++){
                ShortBook b = new ShortBook(rs.getJSONObject(i));
                vec.addElement(b);
            }
        } catch( SQLException | JSONException e ){
            e.printStackTrace();
        }
        return vec;
    }

    public static void getRelated(int i_id, Vector i_id_vec, Vector i_thumbnail_vec ){
        try {
            String stmt = SQL.getRelated;
            JSONArray rs = RESTUtil.executeSelectQuery(builder, stmt, String.valueOf(i_id));
            i_id_vec.removeAllElements();
            i_thumbnail_vec.removeAllElements();

            for(int i = 0; i < rs.length(); i++){
                JSONObject obj = rs.getJSONObject(i);
                //There's a really high chance this doesn't work, test this later
                i_id_vec.addElement(new Integer(obj.getString("i_id")));
                i_thumbnail_vec.addElement(obj.getString("i_thumbnail"));
            }
        }catch( SQLException | JSONException e ){
            e.printStackTrace();
        }
    }

    public static void adminUpdate(int i_id, double cost, String image, String thumbnail){
        try{
            String stmt = SQL.adminUpdate;
            RESTUtil.executeUpdateQuery(builder, stmt, String.valueOf(cost), "'" + image + "'", "'" + thumbnail + "'", String.valueOf(i_id));
            stmt = SQL.adminUpdate_related;
            JSONArray rs = RESTUtil.executeSelectQuery(builder, stmt, String.valueOf(i_id), String.valueOf(i_id));

            int[] related_items = new int[5];
            int counter = 0;
            int last = 0;

            for(int i = 0; i < rs.length(); i++){
                last = rs.getJSONObject(i).getInt("ol_i_id");
                related_items[counter] = last;
                counter++;
            }

            for (int i=counter; i<5; i++) {
                last++;
                related_items[i] = last;
            }

            stmt = SQL.adminUpdate_related1;
            RESTUtil.executeUpdateQuery(builder, stmt,
                                        String.valueOf(related_items[0]),
                                        String.valueOf(related_items[1]),
                                        String.valueOf(related_items[2]),
                                        String.valueOf(related_items[3]),
                                        String.valueOf(related_items[4]),
                                        String.valueOf(i_id));

        } catch( SQLException | JSONException e ) {
            e.printStackTrace();
        }
    }

    public static String GetUserName(int cid){
        String uname = null;
        try {
            String stmt = SQL.getUserName;
            JSONArray rs = RESTUtil.executeSelectQuery(builder, stmt, String.valueOf(cid));
            uname = rs.getJSONObject(0).getString("c_uname");
        } catch( SQLException | JSONException e ){
            e.printStackTrace();
        }
        return uname;
    }

    public static String GetPassword(String cUname){
        String passwd = null;
        try {
            String stmt = SQL.getPassword;
            JSONArray rs = RESTUtil.executeSelectQuery(builder, stmt, "'" + cUname + "'");
            passwd = rs.getJSONObject(0).getString("c_passwd");
        } catch( SQLException | JSONException e ){
            e.printStackTrace();
        }
        return passwd;
    }

    public static Order GetMostRecentOrder(String cUname, Vector order_lines){
        try {
            order_lines.removeAllElements();
            int order_id;
            Order order;

            String stmt = SQL.getMostRecentOrder_id;
            JSONArray rs = RESTUtil.executeSelectQuery(builder, stmt, "'" + cUname + "'");
            if( rs.length() > 0 ) {
                order_id = rs.getJSONObject(0).getInt("o_id");
            } else {
                return null;
            }

            stmt = SQL.getMostRecentOrder_order;
            rs = RESTUtil.executeSelectQuery(builder, stmt, String.valueOf(order_id));
            if( rs.length() > 0 ) {
                order = new Order(rs.getJSONObject(0));
            } else {
                return null;
            }

            stmt = SQL.getMostRecentOrder_lines;
            rs = RESTUtil.executeSelectQuery(builder, stmt, String.valueOf(order_id));
            for(int i = 0; i < rs.length(); i++){
                order_lines.addElement(new OrderLine(rs.getJSONObject(0)));
            }
            return order;
        } catch( SQLException | JSONException e ){
            e.printStackTrace();
        }
        return null;
    }

    public static int createEmptyCart(){
        int SHOPPING_ID = 0; 
        String stmt = SQL.createEmptyCart;
        try { 
            synchronized(Cart.class){
                //TODO: pretty sure this isn't the right way to read a count
                JSONArray rs = RESTUtil.executeSelectQuery(builder, stmt);
                SHOPPING_ID = rs.getJSONObject(0).getInt("COUNT(*)");
                stmt = SQL.createEmptyCart_insert;
                RESTUtil.executeUpdateQuery(builder, stmt);
            }
        } catch( java.lang.Exception ex ) {
            ex.printStackTrace();
        }
        return SHOPPING_ID;
    }


    public static Cart doCart(int SHOPPING_ID, Integer I_ID, Vector ids, Vector quantities){
        Cart cart = null;
        try {
        if( I_ID != null ) {
            addItem(SHOPPING_ID, I_ID.intValue());
        }
        refreshCart(SHOPPING_ID, ids, quantities);
        addRandomItemToCartIfNecessary(SHOPPING_ID);
        resetCartTime(SHOPPING_ID);
        cart = getCart(SHOPPING_ID, 0.0);
        } catch( java.lang.Exception ex ){
            ex.printStackTrace();
        }
        return cart;
    }

    public static void addItem(int SHOPPING_ID, int I_ID){
        String stmt = SQL.addItem;
        try{
            JSONArray rs = RESTUtil.executeSelectQuery(builder, stmt,
                String.valueOf(SHOPPING_ID), String.valueOf(I_ID));
            if( rs.length() > 0 ){
                int currqty = rs.getJSONObject(0).getInt("scl_qty");
                currqty+=1;
                stmt = SQL.addItem_update;
                RESTUtil.executeUpdateQuery(builder, stmt, 
                    String.valueOf(currqty), String.valueOf(SHOPPING_ID), String.valueOf(I_ID));

            } else {
                stmt = SQL.addItem_put;
                RESTUtil.executeUpdateQuery(builder, stmt, String.valueOf(SHOPPING_ID), "1", 
                    String.valueOf(I_ID));
            }
        } catch( java.lang.Exception ex ) {
            ex.printStackTrace();
        }
    }

    public static void refreshCart(int SHOPPING_ID, Vector ids, Vector quantities){
        int i;
        try{
            for(i = 0; i < ids.size(); i++){
                String I_IDstr = (String) ids.elementAt(i);
                String QTYstr = (String) quantities.elementAt(i);
                int I_ID = Integer.parseInt(I_IDstr);
                int QTY = Integer.parseInt(QTYstr);

                if(QTY == 0) {
                    String stmt = SQL.refreshCart_remove;
                    RESTUtil.executeUpdateQuery(builder, stmt, String.valueOf(SHOPPING_ID), String.valueOf(I_ID));
                } else {
                    String stmt = SQL.refreshCart_update;
                    RESTUtil.executeUpdateQuery(builder, stmt, String.valueOf(QTY), String.valueOf(SHOPPING_ID), String.valueOf(I_ID));
                }
            }
        } catch( java.lang.Exception ex ) {
            ex.printStackTrace();
        }
    }

    public static void addRandomItemToCartIfNecessary(int SHOPPING_ID){
        int related_item = 0;
        try{
            String stmt = SQL.addRandomItemToCartIfNecessary;
            JSONArray rs = RESTUtil.executeSelectQuery(builder, stmt, String.valueOf(SHOPPING_ID));
            if( rs.getJSONObject(0).getInt("COUNT(*)") == 0 ){
                int randId = TPCW_Util.getRandomI_ID();
                related_item = getRelated1(randId);
                addItem(SHOPPING_ID, related_item);
            }
        } catch( java.lang.Exception ex ) {
            ex.printStackTrace();
            System.out.println("Adding entry to shopping cart failed: shopping id = " + SHOPPING_ID + " related_item = " + related_item);
        } 
    }

    public static int getRelated1(int I_ID){
        int related1 = -1;
        try{
            String stmt = SQL.getRelated1;
            JSONArray rs = RESTUtil.executeSelectQuery(builder, stmt, String.valueOf(I_ID));
            related1 = rs.getJSONObject(0).getInt("i_related1");
        } catch( java.lang.Exception ex ) {
            ex.printStackTrace();
        }
        return related1;
    }

    public static void resetCartTime(int SHOPPING_ID){
        try{
            String stmt = SQL.resetCartTime;
            RESTUtil.executeUpdateQuery(builder, stmt, String.valueOf(SHOPPING_ID));
        } catch( java.lang.Exception ex ) {
            ex.printStackTrace();
        }
    }

    public static Cart getCart(int SHOPPING_ID, double c_discount){
        Cart myCart = null;
        try{
            String stmt = SQL.getCart;
            JSONArray rs = RESTUtil.executeSelectQuery(builder, stmt, String.valueOf(SHOPPING_ID));
            myCart = new Cart(rs, c_discount);
        } catch( java.lang.Exception ex ) {
            ex.printStackTrace();
        }
        return myCart;
    }

    public static void refreshSession(int C_ID){
        try{
            String stmt = SQL.refreshSession;
            RESTUtil.executeUpdateQuery(builder, stmt, String.valueOf(C_ID));
        } catch( java.lang.Exception ex ) {
            ex.printStackTrace();
        }
    }

    public static Customer createNewCustomer(Customer cust){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            cust.c_discount = (int) (java.lang.Math.random() * 51);
            cust.c_balance =0.0;
            cust.c_ytd_pmt = 0.0;
            // FIXME - Use SQL CURRENT_TIME to do this
            cust.c_last_visit = new Date(System.currentTimeMillis());
            cust.c_since = new Date(System.currentTimeMillis());
            cust.c_login = new Date(System.currentTimeMillis());
            cust.c_expiration = new Date(System.currentTimeMillis() + 
                    7200000);//milliseconds in 2 hours

            cust.addr_id = enterAddress(
                    cust.addr_street1, 
                    cust.addr_street2,
                    cust.addr_city,
                    cust.addr_state,
                    cust.addr_zip,
                    cust.co_name);
            String maxIdStmt = SQL.createNewCustomer_maxId;

            synchronized(Customer.class) {
                // Set parameter
                JSONArray rs = RESTUtil.executeSelectQuery(builder, maxIdStmt);
                //TODO: Possibly not correct
                cust.c_id = rs.getJSONObject(0).getInt("max(c_id)");
                cust.c_id+=1;
                cust.c_uname = TPCW_Util.DigSyl(cust.c_id, 0);
                cust.c_passwd = cust.c_uname.toLowerCase();

                String createNewCustomerStmt = SQL.createNewCustomer;
                RESTUtil.executeUpdateQuery(builder, createNewCustomerStmt,
                        String.valueOf(cust.c_id),
                        "'" + cust.c_uname + "'",
                        "'" + cust.c_passwd + "'",
                        "'" + cust.c_fname + "'",
                        "'" + cust.c_lname + "'",
                        String.valueOf(cust.addr_id),
                        "'" + cust.c_phone + "'",
                        "'" + cust.c_email + "'",
                        "'" + sdf.format(cust.c_since) + "'",
                        "'" + sdf.format(cust.c_last_visit) + "'",
                        "'" + sdf.format(cust.c_login) + "'",
                        "'" + sdf.format(cust.c_expiration) + "'",
                        String.valueOf(cust.c_discount),
                        String.valueOf(cust.c_balance),
                        String.valueOf(cust.c_ytd_pmt),
                        "'" + sdf.format(cust.c_birthdate) + "'",
                        cust.c_data);
            }
        } catch (java.lang.Exception ex) {
            ex.printStackTrace();
        }
        return cust;
    }

    public static int enterAddress(String street1, String street2, String city, String state,
                                   String zip, String country ) {
        int addr_id = 0;
        try{
            String stmt = SQL.enterAddress_id;
            JSONArray rs = RESTUtil.executeSelectQuery(builder, stmt, "'" + country + "'");
            int addr_co_id = rs.getJSONObject(0).getInt("co_id");

            stmt = SQL.enterAddress_match;
            rs = RESTUtil.executeSelectQuery(builder, stmt, "'" + street1 + "'", "'" + street2 + "'", "'" + city + "'", "'" + state + "'",
                                                       "'" + zip + "'", String.valueOf(addr_co_id));
            //Miss on addr table
            if( rs.length() == 0 ) {
                synchronized(Address.class){
                    String getMaxAddrIdStmt = SQL.enterAddress_maxId;
                    JSONArray rs2 = RESTUtil.executeSelectQuery(builder, getMaxAddrIdStmt);
                    //TODO: again with aggregate column naming
                    addr_id = rs2.getJSONObject(0).getInt("max(addr_id)") + 1;
                    String insertAddrStmt = SQL.enterAddress_insert;
                    RESTUtil.executeUpdateQuery(builder, insertAddrStmt, String.valueOf(addr_id),
                                                "'" + street1 + "'", "'" + street2 + "'", "'" + city + "'", "'" + state + "'", "'" + zip + "'",
                                                String.valueOf(addr_co_id));
                }

            } else {
                addr_id = rs.getJSONObject(0).getInt("addr_id");
            }
        } catch( java.lang.Exception ex ) {
            ex.printStackTrace();
        }
        return addr_id;
    }

    public static BuyConfirmResult doBuyConfirm(int shopping_id, int customer_id,
                                                String cc_type, long cc_number,
                                                String cc_name, Date cc_expiry,
                                                String shipping ) {
        BuyConfirmResult result = new BuyConfirmResult();
        try{
            double c_discount = getCDiscount(customer_id);
            result.cart = getCart(shopping_id, c_discount);
            int ship_addr_id = getCAddr(customer_id);
            result.order_id = enterOrder(customer_id, result.cart, ship_addr_id, shipping, c_discount);
            enterCCXact(result.order_id, cc_type, cc_number ,cc_name, cc_expiry, result.cart.SC_TOTAL, ship_addr_id);
            clearCart(shopping_id);
        } catch( java.lang.Exception ex ) {
            ex.printStackTrace();
        }
        return result;
    }

    public static BuyConfirmResult doBuyConfirm(int shopping_id, int customer_id,
                                                String cc_type, long cc_number,
                                                String cc_name, Date cc_expiry,
                                                String shipping, String street_1,
                                                String street_2, String city,
                                                String state, String zip, String country) {
        BuyConfirmResult result = new BuyConfirmResult();
        try{
            double c_discount = getCDiscount(customer_id);
            result.cart = getCart(shopping_id, c_discount);
            int ship_addr_id = enterAddress(street_1, street_2, city, state, zip, country);
            result.order_id = enterOrder(customer_id, result.cart, ship_addr_id, shipping, c_discount);
            enterCCXact(result.order_id, cc_type, cc_number, cc_name, cc_expiry, result.cart.SC_TOTAL, ship_addr_id);
            clearCart(shopping_id);
        } catch( java.lang.Exception ex ) {
            ex.printStackTrace();
        }
        return result;
    }

    public static double getCDiscount(int c_id){
        double c_discount = 0;
        try{
            String stmt = SQL.getCDiscount;
            JSONArray rs = RESTUtil.executeSelectQuery(builder, stmt, String.valueOf(c_id));
            c_discount = rs.getJSONObject(0).getDouble("c_discount");
        } catch( java.lang.Exception ex ) {
            ex.printStackTrace();
        }
        return c_discount;
    }

    public static int getCAddr(int c_id){
        int c_addr_id = 0;
        try{
            String stmt = SQL.getCAddr;
            JSONArray rs = RESTUtil.executeSelectQuery(builder, stmt, String.valueOf(c_id));
            c_addr_id = rs.getJSONObject(0).getInt("c_addr_id");
        } catch( java.lang.Exception ex ) {
            ex.printStackTrace();
        }
        return c_addr_id;
    }

    public static void enterCCXact(int o_id, String cc_type, long cc_number, String cc_name,
                                   Date cc_expiry, double total, int ship_addr_id){

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        if(cc_type.length() > 10)
            cc_type = cc_type.substring(0,10);
        if(cc_name.length() > 30)
            cc_name = cc_name.substring(0,30);
        try{
            String stmt = SQL.enterCCXact;
            RESTUtil.executeUpdateQuery(builder, stmt, String.valueOf(o_id), "'" + cc_type + "'",
                                        String.valueOf(cc_number), "'" + cc_name + "'",
                                        "'" + sdf.format(cc_expiry) + "'", String.valueOf(total),
                                        String.valueOf(ship_addr_id));
        } catch( java.lang.Exception ex ) {
            ex.printStackTrace();
        }
    }

    public static void clearCart(int shopping_id){
        try{
            String stmt = SQL.clearCart;
            RESTUtil.executeUpdateQuery(builder, stmt, String.valueOf(shopping_id));
        } catch( java.lang.Exception ex ) {
            ex.printStackTrace();
        }
    }

    public static int enterOrder(int customer_id, Cart cart, int ship_addr_id,
                                 String shipping, double c_discount) {
        int o_id = 0;
        try{
            String getMaxIdStmt = SQL.enterOrder_maxId;
            synchronized(Order.class){
                JSONArray rs = RESTUtil.executeSelectQuery(builder, getMaxIdStmt);
                //TODO: again with the aggregate field naming
                o_id = rs.getJSONObject(0).getInt("count(o_id)")+1;
                String enterOrderStmt = SQL.enterOrder_insert;
                RESTUtil.executeUpdateQuery(builder, enterOrderStmt, String.valueOf(o_id),
                                            String.valueOf(customer_id),
                                            String.valueOf(cart.SC_SUB_TOTAL),
                                            String.valueOf(cart.SC_TOTAL),
                                            "'" + shipping + "'",
                                            String.valueOf(TPCW_Util.getRandom(7)),
                                            String.valueOf(getCAddr(customer_id)),
                                            String.valueOf(ship_addr_id));
            }

        } catch( java.lang.Exception ex ) {
            ex.printStackTrace();
        }

        Enumeration e = cart.lines.elements();
        int counter = 0;
        while(e.hasMoreElements()) {
            // - Creates one or more 'order_line' rows.
            CartLine cart_line = (CartLine) e.nextElement();
            addOrderLine(counter, o_id, cart_line.scl_i_id, 
                    cart_line.scl_qty, c_discount, 
                    TPCW_Util.getRandomString(20, 100));
            counter++;

            // - Adjusts the stock for each item ordered
            int stock = getStock(cart_line.scl_i_id);
            if ((stock - cart_line.scl_qty) < 10) {
                setStock(cart_line.scl_i_id, 
                        stock - cart_line.scl_qty + 21);
            } else {
                setStock(cart_line.scl_i_id, stock - cart_line.scl_qty);
            }
        }
        return o_id;
    }

    public static void addOrderLine(int ol_id, int ol_o_id, int ol_i_id,
                                    int ol_qty, double ol_discount, String ol_comment ){
        int success = 0;
        try{
            String stmt = SQL.addOrderLine;
            RESTUtil.executeUpdateQuery(builder, stmt, String.valueOf(ol_id),
                                        String.valueOf(ol_o_id), String.valueOf(ol_i_id),
                                        String.valueOf(ol_qty), String.valueOf(ol_discount),
                                        "'" + ol_comment + "'");
        } catch( java.lang.Exception ex ) {
            ex.printStackTrace();
        }
    }

    public static int getStock(int i_id){
        int stock = 0;
        try{
            String stmt = SQL.getStock;
            JSONArray rs = RESTUtil.executeSelectQuery(builder, stmt, String.valueOf(i_id));
            stock = rs.getJSONObject(0).getInt("i_stock");
        } catch( java.lang.Exception ex ) {
            ex.printStackTrace();
        }
        return stock;
    }

    public static void setStock(int i_id, int new_stock){
        try{
            String stmt = SQL.setStock;
            RESTUtil.executeUpdateQuery(builder, stmt,
                                       String.valueOf(i_id), String.valueOf(new_stock));
        } catch( java.lang.Exception ex ) {
            ex.printStackTrace();
        }
    }

    public static void verifyDBConsistency(){
        //LOL NOPE
    }

}

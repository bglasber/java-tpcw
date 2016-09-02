/* 
 * Customer.java - stores the important information for a single customer. 
 *
 ************************************************************************
 *
 * This is part of the the Java TPC-W distribution,
 * written by Harold Cain, Tim Heil, Milo Martin, Eric Weglarz, and Todd
 * Bezenek.  University of Wisconsin - Madison, Computer Sciences
 * Dept. and Dept. of Electrical and Computer Engineering, as a part of
 * Prof. Mikko Lipasti's Fall 1999 ECE 902 course.
 *
 * Copyright (C) 1999, 2000 by Harold Cain, Timothy Heil, Milo Martin, 
 *                             Eric Weglarz, Todd Bezenek.
 *
 * This source code is distributed "as is" in the hope that it will be
 * useful.  It comes with no warranty, and no author or distributor
 * accepts any responsibility for the consequences of its use.
 *
 * Everyone is granted permission to copy, modify and redistribute
 * this code under the following conditions:
 *
 * This code is distributed for non-commercial use only.
 * Please contact the maintainer for restrictions applying to 
 * commercial use of these tools.
 *
 * Permission is granted to anyone to make or distribute copies
 * of this code, either as received or modified, in any
 * medium, provided that all copyright notices, permission and
 * nonwarranty notices are preserved, and that the distributor
 * grants the recipient permission for further redistribution as
 * permitted by this document.
 *
 * Permission is granted to distribute this code in compiled
 * or executable form under the same conditions that apply for
 * source code, provided that either:
 *
 * A. it is accompanied by the corresponding machine-readable
 *    source code,
 * B. it is accompanied by a written offer, with no time limit,
 *    to give anyone a machine-readable copy of the corresponding
 *    source code in return for reimbursement of the cost of
 *    distribution.  This written offer must permit verbatim
 *    duplication by anyone, or
 * C. it is distributed by someone who received only the
 *    executable form, and is accompanied by a copy of the
 *    written offer of source code that they received concurrently.
 *
 * In other words, you are welcome to use, share and improve this codes.
 * You are forbidden to forbid anyone else to use, share and improve what
 * you give them.
 *
 ************************************************************************/

package servlets;

import java.sql.*;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.codehaus.jettison.json.JSONObject;


//glorified struct used for passing customer info around.
public class Customer {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    public int c_id;
    public String c_uname;
    public String c_passwd;
    public String c_fname;
    public String c_lname;
    public String c_phone;
    public String c_email;
    public Date c_since;
    public Date c_last_visit;
    public Date c_login;
    public Date c_expiration;
    public double c_discount;
    public double c_balance;
    public double c_ytd_pmt;
    public Date c_birthdate;
    public String c_data;

    //From the addess table
    public int addr_id;
    public String addr_street1;
    public String addr_street2;
    public String addr_city;
    public String addr_state;
    public String addr_zip;
    public int addr_co_id;

    //From the country table
    public String co_name;
    
    public Customer(){}

    public Customer(ResultSet rs) {
        // The result set should have all of the fields we expect.
        // This relies on using field name access.  It might be a bad
        // way to break this up since it does not allow us to use the
        // more efficient select by index access method.  This also
        // might be a problem since there is no type checking on the
        // result set to make sure it is even a reasonble result set
        // to give to this function.

        try {
            c_id = rs.getInt("c_id");
            c_uname = rs.getString("c_uname");
            c_passwd = rs.getString("c_passwd");
            c_fname = rs.getString("c_fname");
            c_lname = rs.getString("c_lname");

            c_phone = rs.getString("c_phone");
            c_email = rs.getString("c_email");
            c_since = rs.getDate("c_since");
            c_last_visit = rs.getDate("c_last_login");
            c_login = rs.getDate("c_login");
            c_expiration = rs.getDate("c_expiration");
            c_discount = rs.getDouble("c_discount");
            c_balance = rs.getDouble("c_balance");
            c_ytd_pmt = rs.getDouble("c_ytd_pmt");
            c_birthdate = rs.getDate("c_birthdate");
            c_data = rs.getString("c_data");

            addr_id = rs.getInt("addr_id");
            addr_street1 = rs.getString("addr_street1");
            addr_street2 = rs.getString("addr_street2");
            addr_city = rs.getString("addr_city");
            addr_state = rs.getString("addr_state");
            addr_zip = rs.getString("addr_zip");
            addr_co_id = rs.getInt("addr_co_id");

            co_name = rs.getString("co_name");

        } catch (java.lang.Exception ex) {
            ex.printStackTrace();
        }
    }


    public Customer(JSONObject obj) {
        // The result set should have all of the fields we expect.
        // This relies on using field name access.  It might be a bad
        // way to break this up since it does not allow us to use the
        // more efficient select by index access method.  This also
        // might be a problem since there is no type checking on the
        // result set to make sure it is even a reasonble result set
        // to give to this function.

        try {
            c_id = obj.getInt("c_id");
            c_uname = obj.getString("c_uname");
            c_passwd = obj.getString("c_passwd");
            c_fname = obj.getString("c_fname");
            c_lname = obj.getString("c_lname");

            c_phone = obj.getString("c_phone");
            c_email = obj.getString("c_email");
            c_since = sdf.parse(obj.getString("c_since"));
		try {
            c_last_visit = sdf.parse(obj.getString("c_last_login"));
		} catch ( java.lang.Exception ex ) {
			c_last_visit = new Date();
		}
		try {
            c_login = sdf.parse(obj.getString("c_login"));
		} catch( java.lang.Exception ex ) {
			c_login = new Date();
			}

		try {
            c_expiration = sdf.parse(obj.getString("c_expiration"));
		} catch( java.lang.Exception ex ) {
			c_expiration = new Date();
			}

            c_discount = obj.getDouble("c_discount");
            c_balance = obj.getDouble("c_balance");
            c_ytd_pmt = obj.getDouble("c_ytd_pmt");
		try {
            c_birthdate = sdf.parse(obj.getString("c_birthdate"));
		} catch( java.lang.Exception ex ) {
			c_birthdate = new Date();
			}

            c_data = obj.getString("c_data");

            addr_id = obj.getInt("addr_id");
            addr_street1 = obj.getString("addr_street1");
            addr_street2 = obj.getString("addr_street2");
            addr_city = obj.getString("addr_city");
            addr_state = obj.getString("addr_state");
            addr_zip = obj.getString("addr_zip");
            addr_co_id = obj.getInt("addr_co_id");

            co_name = obj.getString("co_name");

        } catch (java.lang.Exception ex) {
            ex.printStackTrace();
        }
    }
}

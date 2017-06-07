package servlets;

import java.io.*;
import java.util.*;
import javax.servlet.*;
import javax.servlet.http.*;

import common.SQL;

public class TPCW_dm_interaction extends HttpServlet {
  public void doGet(HttpServletRequest req, HttpServletResponse res)
      throws IOException, ServletException {

    /*SERVLET SETUP*/
    HttpSession session = req.getSession(false);
    if (session == null)
      session = req.getSession(true);
    // This must be after the getSession() call.
    PrintWriter out = res.getWriter();
    // Set the content type of this servlet's result.
    res.setContentType("text/html");

    TPCW_DM.initialize();

    // Generate Home Page Head
    out.print("<HTML> <HEAD> <TITLE>TPC-W DM Page</TITLE></HEAD>\n");
    out.print("<BODY BGCOLOR=\"#ffffff\">\n");
    out.print("<H1 ALIGN=\"center\">TPC Web Commerce Benchmark (TPC-W)</H1>\n");
    out.print("<H1 ALIGN=\"center\">DYNA MAST INIT'd</H1>\n");
    // Generate Trailer
    out.print("<hr><font size=-1>\n");
    out.print(
        "<a href=\"http://www.tpc.org/miscellaneous/TPC_W.folder/Company_Public_Review.html\">TPC-W Benchmark</a>,\n");
    out.print(
        "<a href=\"http://www.cae.wisc.edu/~mikko/ece902.html\">ECE 902</a>,\n");
    out.print(
        "<a href=\"http://www.cs.wisc.edu/~arch/uwarch\">University of Wisconsin Computer Architecture</a>,November 1999.\n");
    out.print("</font> </BODY> </HTML>\n");
    out.close();
    return;
  }

}

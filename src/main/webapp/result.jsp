<%@ page import="java.util.ArrayList" %>
<%@ page import="java.util.Map" %>
<%@ page import="java.util.Iterator" %>

<%@ page contentType="text/html;charset=UTF-8" language="java" %>

<%
    String queryBack = (String) request.getAttribute("queryBack");
    ArrayList<Map<String, Object>> newslist = (ArrayList<Map<String, Object>>) request.getAttribute("newslist");
    String totalHits = (String) request.getAttribute("totalHits");
    String totalTime = (String) request.getAttribute("totalTime");
    int pages = Integer.parseInt(totalHits) / 10 + 1;
    pages = pages > 10 ? 10 : pages;
%>
<html>
<head>
    <title>搜尋結果</title>
    <link type="text/css" rel="stylesheet" href="css/result.css">
</head>
<body>
<div class="result_search">
    <div class="logo">
        <h2><a href="index.jsp">新聞搜尋</a></h2>
    </div>
    <div class="searchbox">
        <form action="/SearchNews" method="get">
            <input type="text" name="query" value="<%=queryBack%>">
            <input type="submit" value="搜尋一下">
        </form>
    </div>
</div>
<h5 class="result_info">共搜尋到<span><%=totalHits%></span>條結果,耗時<span> <%=Double.parseDouble(totalTime) / 1000.0 %></span>秒
</h5>
<div class="newslist">
    <%
        if (newslist.size() > 0) {
            Iterator<Map<String, Object>> iter = newslist.iterator();
            while (iter.hasNext()) {
                Map<String, Object> news = iter.next();
                String content = news.get("content").toString();
                content = content.length() > 200 ? content.substring(0, 200) : content;
    %>
    <div class="news">
        <h4><a href="<%=news.get("url")%>"><%=news.get("title")%>
        </a></h4>
        <p><%=content%> ...
        </p>
    </div>
    <%
            }
        }
    %>
</div>
<div class="page">
    <ul>
        <%
            for (int i = 1; i <= pages; i++) {
        %>
        <li><a href="/SearchNews?query=<%=queryBack%>&pageNum=<%=i%>"><%=i%>
        </a></li>
        <%
            }
        %>

    </ul>
</div>
<div class="info">
    <p>新聞搜尋專案實戰 Powered By <b> Elasticsearch</b></p>
    <p>@2017 All right reserved</p>
</div>
</body>
</html>

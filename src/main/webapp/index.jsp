<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8" import="java.util.Calendar"%>

<%
	Calendar cal = Calendar.getInstance();
	int year = cal.get(Calendar.YEAR); //取得年份
%>


<!DOCTYPE html>
<html>
<head>
    <title>新聞搜尋</title>
    <link type="text/css" rel="stylesheet" href="css/index.css">
</head>
<body>
<div class="box">
    <h1>Elasticsearch新聞搜尋</h1>
    <div class="searchbox">
        <form action="/SearchNews" method="get">
            <input type="text" name="query">
            <input type="submit" value="搜尋一下">
        </form>
    </div>
</div>
</body>
</html>

/****** Script for SelectTopNRows command from SSMS  ******/
use gplus_refined

SELECT [dim]
      ,[v_size]
      ,[e_size],
	  ([v_size] + [e_size]) AS [size]
  FROM [gplus_refined].[dbo].[dim_info]
  ORDER BY ([v_size] + [e_size]) DESC
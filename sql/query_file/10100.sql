use gplus_refined
GO;


DROP TABLE IF EXISTS [10100_node_aggregated]
SELECT 
	[gender],[place],
	CONCAT([gender],'.',[place])
	AS [aggregated_id],
	SUM([weight]) AS [weight]
INTO [10100_node_aggregated]
FROM [dbo].[11100_node_aggregated]
GROUP BY 
	[gender],[place]
GO;


DROP TABLE IF EXISTS [10100_id_map]
SELECT 
	CONCAT([gender],'.',[place])
AS [aggregated_id], [aggregated_id] AS [indexed_id] INTO [10100_id_map]
FROM [dbo].[11100_node_aggregated]
GO;


DROP TABLE IF EXISTS [10100_edge_map]
SELECT [start_map].[aggregated_id] AS [start], [end_map].[aggregated_id] AS [end], [dbo].[11100_edge_aggregated].[weight]
INTO [dbo].[10100_edge_map]
FROM [dbo].[11100_edge_aggregated]
JOIN [dbo].[10100_id_map] AS [start_map]
ON [dbo].[11100_edge_aggregated].[start] = [start_map].[indexed_id]
JOIN [dbo].[10100_id_map] AS [end_map]
ON [dbo].[11100_edge_aggregated].[end] = [end_map].[indexed_id]
GO;


DROP TABLE IF EXISTS [10100_edge_aggregated]
SELECT [start], [end], SUM([weight]) AS [weight] INTO [10100_edge_aggregated]
FROM [dbo].[10100_edge_map]
GROUP BY [start],[end]
GO;


DROP TABLE IF EXISTS [10100_id_map]
DROP TABLE IF EXISTS [10100_edge_map]
GO;


IF '10100' NOT IN (
	SELECT [dim]
	FROM [dbo].[dim_info]
	)
	INSERT INTO [dbo].[dim_info]([dim],[v_size],[e_size]) 
	VALUES (
		'10100',
		(SELECT COUNT(*) FROM [dbo].[10100_node_aggregated]),
		(SELECT COUNT(*) FROM [dbo].[10100_edge_aggregated])
		);
ELSE
	UPDATE [dbo].[dim_info]
	SET [v_size]=(SELECT COUNT(*) FROM [dbo].[10100_node_aggregated]),
		[e_size]=(SELECT COUNT(*) FROM [dbo].[10100_edge_aggregated])
	WHERE
		[dim]='10100';

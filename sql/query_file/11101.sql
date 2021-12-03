use gplus_refined
GO;


DROP TABLE IF EXISTS [11101_node_aggregated]
SELECT 
	[gender],[job],[place],[institution],
	CONCAT([gender],'.',[job],'.',[place],'.',[institution])
	AS [aggregated_id],
	SUM([weight]) AS [weight]
INTO [11101_node_aggregated]
FROM [dbo].[11111_node_aggregated]
GROUP BY 
	[gender],[job],[place],[institution]
GO;


DROP TABLE IF EXISTS [11101_id_map]
SELECT 
	CONCAT([gender],'.',[job],'.',[place],'.',[institution])
AS [aggregated_id], [aggregated_id] AS [indexed_id] INTO [11101_id_map]
FROM [dbo].[11111_node_aggregated]
GO;


DROP TABLE IF EXISTS [11101_edge_map]
SELECT [start_map].[aggregated_id] AS [start], [end_map].[aggregated_id] AS [end], [dbo].[11111_edge_aggregated].[weight]
INTO [dbo].[11101_edge_map]
FROM [dbo].[11111_edge_aggregated]
JOIN [dbo].[11101_id_map] AS [start_map]
ON [dbo].[11111_edge_aggregated].[start] = [start_map].[indexed_id]
JOIN [dbo].[11101_id_map] AS [end_map]
ON [dbo].[11111_edge_aggregated].[end] = [end_map].[indexed_id]
GO;


DROP TABLE IF EXISTS [11101_edge_aggregated]
SELECT [start], [end], SUM([weight]) AS [weight] INTO [11101_edge_aggregated]
FROM [dbo].[11101_edge_map]
GROUP BY [start],[end]
GO;


DROP TABLE IF EXISTS [11101_id_map]
DROP TABLE IF EXISTS [11101_edge_map]
GO;


IF '11101' NOT IN (
	SELECT [dim]
	FROM [dbo].[dim_info]
	)
	INSERT INTO [dbo].[dim_info]([dim],[v_size],[e_size]) 
	VALUES (
		'11101',
		(SELECT COUNT(*) FROM [dbo].[11101_node_aggregated]),
		(SELECT COUNT(*) FROM [dbo].[11101_edge_aggregated])
		);
ELSE
	UPDATE [dbo].[dim_info]
	SET [v_size]=(SELECT COUNT(*) FROM [dbo].[11101_node_aggregated]),
		[e_size]=(SELECT COUNT(*) FROM [dbo].[11101_edge_aggregated])
	WHERE
		[dim]='11101';

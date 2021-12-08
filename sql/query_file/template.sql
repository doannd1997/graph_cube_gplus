DROP TABLE IF EXISTS [%%des_dim%%_node_aggregated]
SELECT 
	%%dim_columns%%,
	%%dim_aggregated%%
	AS [aggregated_id],
	SUM([weight]) AS [weight]
INTO [%%des_dim%%_node_aggregated]
FROM [dbo].[%%src_dim%%_node_aggregated]
GROUP BY 
	%%dim_columns%%
GO;


DROP TABLE IF EXISTS [%%des_dim%%_id_map]
SELECT 
	%%dim_aggregated%%
AS [aggregated_id], [aggregated_id] AS [indexed_id] INTO [%%des_dim%%_id_map]
FROM [dbo].[%%src_dim%%_node_aggregated]
GO;


DROP TABLE IF EXISTS [%%des_dim%%_edge_map]
SELECT [start_map].[aggregated_id] AS [start], [end_map].[aggregated_id] AS [end], [dbo].[%%src_dim%%_edge_aggregated].[weight]
INTO [dbo].[%%des_dim%%_edge_map]
FROM [dbo].[%%src_dim%%_edge_aggregated]
JOIN [dbo].[%%des_dim%%_id_map] AS [start_map]
ON [dbo].[%%src_dim%%_edge_aggregated].[start] = [start_map].[indexed_id]
JOIN [dbo].[%%des_dim%%_id_map] AS [end_map]
ON [dbo].[%%src_dim%%_edge_aggregated].[end] = [end_map].[indexed_id]
GO;


DROP TABLE IF EXISTS [%%des_dim%%_edge_aggregated]
SELECT [start], [end], SUM([weight]) AS [weight] INTO [%%des_dim%%_edge_aggregated]
FROM [dbo].[%%des_dim%%_edge_map]
GROUP BY [start],[end]
GO;


DROP TABLE IF EXISTS [%%des_dim%%_id_map]
DROP TABLE IF EXISTS [%%des_dim%%_edge_map]
GO;


IF '%%des_dim%%' NOT IN (
	SELECT [dim]
	FROM [dbo].[dim_info]
	)
	INSERT INTO [dbo].[dim_info]([dim],[v_size],[e_size]) 
	VALUES (
		'%%des_dim%%',
		(SELECT COUNT(*) FROM [dbo].[%%des_dim%%_node_aggregated]),
		(SELECT COUNT(*) FROM [dbo].[%%des_dim%%_edge_aggregated])
		);
ELSE
	UPDATE [dbo].[dim_info]
	SET [v_size]=(SELECT COUNT(*) FROM [dbo].[%%des_dim%%_node_aggregated]),
		[e_size]=(SELECT COUNT(*) FROM [dbo].[%%des_dim%%_edge_aggregated])
	WHERE
		[dim]='%%des_dim%%';

use gplus_refined
GO;

DROP TABLE IF EXISTS [11111_node_aggregated]
SELECT 
	COUNT(*) as 'weight',
	gender,
	job,
	place,
	university,
	institution,
	CONCAT(gender, '.', job, '.', place, '.', university, '.', institution) as 'aggregated_id'
INTO [11111_node_aggregated]
FROM [dbo].[node_attr]
GROUP BY [gender], [job], [place], [university], [institution]
GO;

DROP TABLE IF EXISTS [11111_id_map]
SELECT CONCAT(gender, '.', job, '.', place, '.', university, '.', institution) as 'aggregated_id', indexed_id INTO [11111_id_map]
FROM [dbo].[node_attr]
GO;

DROP TABLE IF EXISTS [11111_edge_map]
SELECT [start_map].[aggregated_id] as [start], [end_map].[aggregated_id] as [end], 1 AS [weight] INTO [11111_edge_map]
FROM [dbo].[edge] 
JOIN [dbo].[11111_id_map] as [start_map]
ON [dbo].[edge].[start] = [start_map].[indexed_id]
JOIN [dbo].[11111_id_map] as [end_map]
ON [dbo].[edge].[end] = [end_map].[indexed_id]
GO;

SELECT COUNT(*)
FROM [11111_edge_map]
GO;

DROP TABLE IF EXISTS [11111_edge_aggregated]
SELECT [start], [end], SUM([weight]) AS [weight] INTO [11111_edge_aggregated]
FROM [11111_edge_map]
GROUP BY [start], [end]
GO;

SELECT COUNT(*)
FROM [11111_edge_aggregated]
GO;

INSERT INTO [dbo].[dim_info] ([dim], [v_size], [e_size])
VALUES (
	11111,
	(SELECT COUNT(*) FROM [dbo].[11111_node_aggregated]),
	(SELECT COUNT(*) FROM [dbo].[11111_edge_aggregated])
	)
GO;
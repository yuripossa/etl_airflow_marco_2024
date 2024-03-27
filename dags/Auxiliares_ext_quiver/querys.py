#region Consulta nbs_clientes
query_quiver_clientes = """

SELECT
       T1.SISTEMAORIGEM
      ,T1.CODCLIENTE
      ,T1.CLIENTE
      ,T1.TIPOENDERECO
      ,T1.ENDERECOPRINCIPAL
      ,T1.UF
      ,T1.CIDADE
      ,T1.BAIRRO
      ,T1.LOGRADOURO
      ,T1.NUMERO
      ,T1.COMPLEMENTO
      ,T1.CEP
      ,T1.EMAIL
      ,T1.TELL
      ,T1.CELULAR AS CELL
      ,T1.TIPOFJ
      ,T1.DOCCLI
      ,T1.NIVEL
      ,T1.DIVISAO
	 
      ,T1.GRUPO_HIERARQUICO
      ,T1.DATA_CADASTRO
      ,T1.DATA_ULTIMA_ATUALIZACAO    

FROM
    (   SELECT 
              'QUIVER' AS SISTEMAORIGEM
              ,c.Cliente AS CODCLIENTE
			  ,case when c.Tipo_pessoa = 'J'
					then right(trim(replace(replace(replace(c.Cgc_cpf, '.', ''), '-', ''), '/', '')),14)
					Else trim(replace(replace(replace(c.Cgc_cpf, '.', ''), '-', ''), '/', '')) 
				end as DOCCLI
              ,c.Nome AS CLIENTE
			  ----******************
		,c.NIVEL
			  
		,c.DIVISAO
			  
		,c.GRUPO_HIERARQUICO
			  ---********************
              ,te.Descricao AS TIPOENDERECO
              ,CASE WHEN e.End_corresp = 1 THEN 'S' ELSE 'N' END AS ENDERECOPRINCIPAL
              ,e.Estado AS UF
              ,e.Cidade AS CIDADE
              ,e.Bairro AS BAIRRO
              ,e.Endereco as LOGRADOURO
              ,e.Numero as NUMERO
              ,e.Complemento as COMPLEMENTO
              ,replace(replace(e.Cep, '-', ''), '.', '') AS CEP

              ,CASE WHEN c.Tipo_pessoa = 'F' THEN 'FISICA'
                    WHEN c.Tipo_pessoa = 'J' THEN 'JURIDICA' 
                    END AS TIPOFJ
              ,c.E_mail AS EMAIL

              ,(SELECT TOP 1 (ISNULL(DDD,'') + ISNULL(Telefone,''))
                FROM Tabela_ClientFones 
                WHERE Cliente = c.Cliente and Tipo_telefone = 4 and Situacao = 'A'
                ORDER BY Data_inclusao DESC) AS CELULAR

              ,(SELECT TOP 1 ISNULL(DDD,'') + ISNULL(Telefone,'') 
                FROM Tabela_ClientFones 
                WHERE Cliente = c.Cliente and Tipo_telefone = 1 and Situacao = 'A'
                ORDER BY Data_inclusao DESC) AS TELL

              ,c.Data_inclusao AS DATA_CADASTRO
              ,c.Data_alteracao AS DATA_ULTIMA_ATUALIZACAO

        FROM Tabela_Clientes c

        LEFT JOIN ( SELECT cliente, MAX(Data_inclusao) as Data_inclusao
                    FROM Tabela_ClientEnder 
                    GROUP BY Cliente ) ult_e on ult_e.cliente = c.cliente  -- Endereço de ultima atualização.

        LEFT JOIN Tabela_ClientEnder e 
        ON
        e.Cliente = C.cliente and e.Cliente = ult_e.Cliente and e.Data_inclusao = ult_e.Data_inclusao

        LEFT JOIN Tabela_TiposEndereco te 
        ON
        te.Tipo_endereco = e.Tipo_endereco

        WHERE EXISTS (SELECT * FROM Tabela_Documentos WHERE Cliente = c.Cliente )
                AND c.Cgc_cpf NOT IN ('')
) AS T1
"""
#endregion

#region Deletes
deleteQuiverClientes = """
	DELETE FROM staging.ext_clientes_quiver
"""
#endregion
package simpleanonymizer

import simpleanonymizer.SlickProfile.api._

import slick.dbio.DBIO
import slick.jdbc.meta.{MColumn, MForeignKey, MQName, MTable}

import scala.concurrent.ExecutionContext

class DbMetadata(schema: String)(implicit executionContext: ExecutionContext) {

  /** Get all tables in the specified schema */
  def getAllTables: DBIO[Seq[MTable]] =
    MTable.getTables(None, Some(schema), None, Some(Seq("TABLE"))).map { tables =>
      tables.sortBy(_.name.name)
    }

  /** Get all foreign key relationships from the database */
  def getAllForeignKeys: DBIO[Seq[MForeignKey]] =
    MTable.getTables(None, Some(schema), None, Some(Seq("TABLE"))).flatMap { tables =>
      DBIO.traverse(tables)(_.getImportedKeys).map(_.flatten)
    }

  /** Get all column names for all tables in the schema, grouped by table name. */
  def getAllColumns: DBIO[Map[String, Seq[String]]] =
    MColumn.getColumns(MQName(None, Some(schema), "%"), "%").map { columns =>
      columns.groupBy(_.table.name).map { case (table, cols) => table -> cols.map(_.name) }
    }

  /** Get all primary key column names for all tables in the schema, grouped by table name. */
  def getAllPrimaryKeys: DBIO[Map[String, Set[String]]] =
    sql"""
      SELECT kcu.table_name, kcu.column_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
      WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = $schema
    """
      .as[(String, String)]
      .map(_.groupBy(_._1).map { case (table, cols) => table -> cols.map(_._2).toSet })
}

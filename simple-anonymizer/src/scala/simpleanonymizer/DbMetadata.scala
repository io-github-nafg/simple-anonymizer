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
      SELECT c.relname, a.attname
      FROM pg_index i
      JOIN pg_class c ON c.oid = i.indrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
      WHERE i.indisprimary AND n.nspname = $schema
    """
      .as[(String, String)]
      .map(_.groupBy(_._1).map { case (table, cols) => table -> cols.map(_._2).toSet })
}

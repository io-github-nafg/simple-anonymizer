package simpleanonymizer

import slick.dbio.DBIO
import slick.jdbc.meta.{MForeignKey, MTable}

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
}

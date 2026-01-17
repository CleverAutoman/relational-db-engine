package database.cli.visitor;

/**
 * Purely symbolic class
 */
class BeginStatementVisitor extends StatementVisitor {
    public StatementType getType() {
        return StatementType.BEGIN;
    }
}
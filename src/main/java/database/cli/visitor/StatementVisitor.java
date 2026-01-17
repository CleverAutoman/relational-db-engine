package database.cli.visitor;

import database.Transaction;
import database.cli.parser.RookieParserDefaultVisitor;
import database.query.QueryPlan;

import java.io.PrintStream;
import java.util.Optional;

abstract class StatementVisitor extends RookieParserDefaultVisitor {
    public void execute(Transaction transaction, PrintStream out) {
        throw new UnsupportedOperationException("Statement is not executable.");
    }

    public Optional<String> getSavepointName() {
        return Optional.empty();
    }

    public Optional<QueryPlan> getQueryPlan(Transaction transaction) {
        return Optional.empty();
    }

    public abstract StatementType getType();
}
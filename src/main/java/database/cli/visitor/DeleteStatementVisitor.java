package database.cli.visitor;

import database.Transaction;
import database.cli.parser.ASTExpression;
import database.cli.parser.ASTIdentifier;
import database.query.expr.Expression;
import database.query.expr.ExpressionVisitor;
import database.table.Schema;

import java.io.PrintStream;

class DeleteStatementVisitor extends StatementVisitor {
    public String tableName;
    public Expression cond;

    @Override
    public void visit(ASTIdentifier node, Object data) {
        this.tableName = (String) node.jjtGetValue();
    }

    @Override
    public void visit(ASTExpression node, Object data) {
        ExpressionVisitor visitor = new ExpressionVisitor();
        node.jjtAccept(visitor, data);
        this.cond = visitor.build();
    }

    @Override
    public void execute(Transaction transaction, PrintStream out) {
        try {
            Schema schema = transaction.getSchema(tableName);
            this.cond.setSchema(schema);
            transaction.delete(tableName, cond::evaluate);
            out.println("DELETE");
        } catch (Exception e) {
            out.println(e.getMessage());
            out.println("Failed to execute DELETE.");
        }
    }

    @Override
    public StatementType getType() {
        return StatementType.DELETE;
    }
}
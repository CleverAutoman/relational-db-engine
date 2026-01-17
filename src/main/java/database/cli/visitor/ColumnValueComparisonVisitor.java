package database.cli.visitor;

import database.cli.PrettyPrinter;
import database.cli.parser.ASTColumnName;
import database.cli.parser.ASTComparisonOperator;
import database.cli.parser.ASTLiteral;
import database.cli.parser.RookieParserDefaultVisitor;
import database.common.PredicateOperator;
import database.databox.DataBox;

class ColumnValueComparisonVisitor extends RookieParserDefaultVisitor {
    PredicateOperator op;
    String columnName;
    DataBox value;

    @Override
    public void visit(ASTLiteral node, Object data) {
        this.value = PrettyPrinter.parseLiteral((String) node.jjtGetValue());
    }

    @Override
    public void visit(ASTColumnName node, Object data) {
        this.columnName = (String) node.jjtGetValue();
        // keep things in format columnName <= value
        if (this.op != null) this.op = op.reverse();
    }

    @Override
    public void visit(ASTComparisonOperator node, Object data) {
        this.op = PredicateOperator.fromSymbol((String) node.jjtGetValue());
    }
}

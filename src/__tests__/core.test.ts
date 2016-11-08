
import * as chai from 'chai';
import * as chaiAsPromised from 'chai-as-promised';
import * as knex from 'knex';

import {
    defineBoolean,
    defineDatetime,
    defineJson,
    defineMapping,
    defineNumber,
    defineString,

    equal,

    Mapper
} from 'simple-typed-sql';

import {
    createJoinQuery,
    SqlGraphTopologyError,
    SqlGraph    
} from '../';

chai.use(chaiAsPromised);

let expect = chai.expect;

describe("SQL Graph", function () {

    let mappings = {
        orderDetail: defineMapping(
            'order_detail',
            {
                id: defineNumber(),
                orderId: defineNumber(),
                productId: defineNumber(),
                quantity: defineNumber(),
                unitPrice: defineNumber()
            }
        ),

        order: defineMapping(
            'order',
            {
                id: defineNumber()
            }
        ),

        product: defineMapping(
            'product',
            {
                id: defineNumber(),
                categoryId: defineNumber()
            }
        ),

        category: defineMapping(
            'category',
            {
                id: defineNumber(),
                name: defineString()
            }
        )
    };

    
    let knexClient = knex({
        client: 'pg'
    });

    let mapper = new Mapper(knexClient);

    let sqlGraph = new SqlGraph();

    sqlGraph.addMasterDetailJoin(mappings.orderDetail, mappings.order, equal(mappings.orderDetail.orderId, mappings.order.id));
    sqlGraph.addMasterDetailJoin(mappings.orderDetail, mappings.product, equal(mappings.orderDetail.productId, mappings.product.id));
    sqlGraph.addMasterDetailJoin(mappings.product, mappings.category, equal(mappings.product.categoryId, mappings.category.id));

    it("should compute simple join paths", function () {
        let joinPath = sqlGraph.getJoinPath(mappings.orderDetail, mappings.category);

        expect(joinPath).to.not.be.null;
        expect(joinPath.length).to.equal(2);
    });

    it("should error on non-existing join paths", function () {
        expect(function () { sqlGraph.getJoinPath(mappings.category, mappings.order) }).to.throw(SqlGraphTopologyError);
    });

    it("should generate empty join paths for identity mappings", function () {
        let joinPath = sqlGraph.getJoinPath(mappings.orderDetail, mappings.orderDetail);
        expect(joinPath.length).to.equal(0);
    });

    it("should generate from queries with joins corresponding to the join path", function () {
        let joinPath = sqlGraph.getJoinPath(mappings.orderDetail, mappings.category);

        let query = createJoinQuery(mapper, joinPath)
            .select({
                orderDetailId: mappings.orderDetail.id,
                categoryId: mappings.category.id
            });

        expect(query.getKnexQuery().toQuery()).to.equal('select "order_detail"."id" as "orderDetailId", "category"."id" as "categoryId" from "order_detail" inner join "product" on "order_detail"."productId" = "product"."id" inner join "category" on "product"."categoryId" = "category"."id"');
    });

});



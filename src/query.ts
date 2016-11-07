
import { Mapper } from 'simple-typed-sql';

import { FromQuery } from 'simple-typed-sql/lib/core';

import { JoinPath } from './sql_graph';

export function createJoinQuery(mapper: Mapper, joinPath: JoinPath) {
    return joinPath.reduce((query, join) => {
        return query.innerJoin(join.detailMapping, join.joinCondition);

    }, mapper.from(joinPath[0].masterMapping));
}

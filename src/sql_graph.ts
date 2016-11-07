
import * as graphlib from 'graphlib';
import {
    ConditionClause,
    Mapping,
    WrappedMappingData
} from 'simple-typed-sql';

import { CustomError } from './common';

export interface Join<T, U> {
    masterMapping: Mapping<T>,
    detailMapping: Mapping<U>,
    joinCondition: ConditionClause
}

export type JoinPath = Join<void, void>[];

export class SqlGraph {
    private graph = new graphlib.Graph();

    constructor() {
    }

    addMasterDetailJoin<T, U>(
        masterMapping: Mapping<T>,
        detailMapping: Mapping<U>,
        joinCondition: ConditionClause
    ) {
        let masterTableName = WrappedMappingData.getMapping(masterMapping).getTableName();
        let detailTableName = WrappedMappingData.getMapping(detailMapping).getTableName();

        this.graph.setNode(masterTableName, masterMapping);
        this.graph.setNode(detailTableName, detailMapping);

        this.graph.setEdge(masterTableName, detailTableName, joinCondition);
    }

    addJoin<T, U>(join: Join<T, U>) {
        this.addMasterDetailJoin(join.masterMapping, join.detailMapping, join.joinCondition);
    }

    /**
     * Return a list of joins linking the source relation to the target.
     */
    getJoinPath<T, U>(sourceMapping: Mapping<T>, targetMapping: Mapping<U>): JoinPath {
        let sourceTableName = WrappedMappingData.getMapping(sourceMapping).getTableName();
        let targetTableName = WrappedMappingData.getMapping(targetMapping).getTableName();

        let edgeData = graphlib.alg.dijkstra(this.graph, sourceTableName);
        
        let edgeTargetNode = targetTableName;

        let joinPath: Join<void, void>[] = []; 
        while (edgeTargetNode !== sourceTableName) {
            let edge = edgeData[edgeTargetNode];
            
            let edgeSourceNode = edge.predecessor;

            if (!edgeSourceNode) {
                throw new SqlGraphTopologyError("No join path found.");
            }

            joinPath.push({
                masterMapping: this.getNodeMapping(edgeSourceNode),
                detailMapping: this.getNodeMapping(edgeTargetNode),
                joinCondition: this.getEdgeJoinCondition(edgeSourceNode, edgeTargetNode)
            })

            edgeTargetNode = edgeSourceNode;
        }

        return joinPath.reverse();
    }

    private getNodeMapping(tableName: string) {
        return this.graph.node(tableName) as Mapping<void>;
    }

    private getEdgeJoinCondition(sourceTableName: string, targetTableName: string) {
        return this.graph.edge(sourceTableName, targetTableName) as ConditionClause;
    }

}

export class SqlGraphTopologyError extends CustomError {}

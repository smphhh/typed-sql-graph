
import * as graphlib from 'graphlib';
import {
    BaseMappingData,
    ConditionClause,
    Mapping,
    WrappedMappingData,
    equal
} from 'simple-typed-sql';
import { ComparisonValueType, ComparisonOperandType } from 'simple-typed-sql/lib/condition';
import { AttributeDefinition } from 'simple-typed-sql/lib/mapping';

import { CustomError } from './common';

export interface Join<T, U> {
    masterMapping: Mapping<T>,
    detailMapping: Mapping<U>,
    joinCondition: ConditionClause
}

export type JoinPath = Join<{}, {}>[];

export class SqlGraph {
    private graph = new graphlib.Graph();

    constructor() {
    }

    addMasterDetailJoin<T, U>(
        masterMapping: Mapping<T>,
        detailMapping: Mapping<U>,
        joinCondition: ConditionClause
    ) {
        let masterTableName = WrappedMappingData.getMappingData(masterMapping).getTableName();
        let detailTableName = WrappedMappingData.getMappingData(detailMapping).getTableName();

        this.graph.setNode(masterTableName, masterMapping);
        this.graph.setNode(detailTableName, detailMapping);

        this.graph.setEdge(masterTableName, detailTableName, joinCondition);

        let join: Join<T, U> = {
            masterMapping,
            detailMapping,
            joinCondition
        };

        return join;
    }

    addSimpleMasterDetailJoin<T extends ComparisonValueType>(masterAttribute: T, detailAttribute: T) {
        let masterAttributeDefinition: ComparisonOperandType = masterAttribute;
        let detailAttributeDefinition: ComparisonOperandType = detailAttribute;

        if (masterAttributeDefinition instanceof AttributeDefinition && detailAttributeDefinition instanceof AttributeDefinition) {
            return this.addMasterDetailJoin(
                masterAttributeDefinition.mapping,
                detailAttributeDefinition.mapping,
                equal(masterAttribute, detailAttribute)
            );
        } else {
            throw Error("Join attributes must be of type AttributeDefinition.");
        }        
    }

    addJoin<T, U>(join: Join<T, U>) {
        this.addMasterDetailJoin(join.masterMapping, join.detailMapping, join.joinCondition);
    }

    /**
     * Return a list of joins linking the source relation to the target.
     */
    getJoinPath<T, U>(sourceMapping: Mapping<T>, targetMapping: Mapping<U>): JoinPath {
        let sourceTableName = WrappedMappingData.getMappingData(sourceMapping).getTableName();
        let targetTableName = WrappedMappingData.getMappingData(targetMapping).getTableName();

        let edgeData = graphlib.alg.dijkstra(this.graph, sourceTableName);
        
        let edgeTargetNode = targetTableName;

        let joinPath: Join<{}, {}>[] = []; 
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
        return this.graph.node(tableName) as Mapping<{}>;
    }

    private getEdgeJoinCondition(sourceTableName: string, targetTableName: string) {
        return this.graph.edge(sourceTableName, targetTableName) as ConditionClause;
    }

}

export class SqlGraphTopologyError extends CustomError {}

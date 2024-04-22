/*
 * Copyright (C) ActiveViam 2023-2024
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.apps.cfg;

import static com.qfs.literal.ILiteralType.INT;
import static com.qfs.literal.ILiteralType.STRING;

import java.util.Collection;
import java.util.LinkedList;

import org.springframework.context.annotation.Configuration;

import com.activeviam.apps.constants.StoreAndFieldConstants;
import com.activeviam.builders.StartBuilding;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.desc.IReferenceDescription;
import com.qfs.desc.IStoreDescription;
import com.qfs.desc.impl.DatastoreSchemaDescription;
import com.qfs.desc.impl.StoreDescription;
import com.qfs.server.cfg.IDatastoreSchemaDescriptionConfig;

import lombok.RequiredArgsConstructor;

@Configuration
@RequiredArgsConstructor
public class DatastoreSchemaConfig implements IDatastoreSchemaDescriptionConfig {

    private IStoreDescription createPositionsStoreDescription() {
        return StoreDescription.builder()
                .withStoreName(StoreAndFieldConstants.POSITIONS)
                .withField(StoreAndFieldConstants.POSITIONS_ID, INT)
                .asKeyField()
                .withField(StoreAndFieldConstants.POSITIONS_SECURITY_ID, INT)
                .withField(StoreAndFieldConstants.POSITIONS_CCP_L1, STRING)
                .withField(StoreAndFieldConstants.POSITIONS_CCP_L2, STRING)
                .withField(StoreAndFieldConstants.POSITIONS_CCP_L3, STRING)
                .withField(StoreAndFieldConstants.POSITIONS_CCP_L4, STRING)
                .withField(StoreAndFieldConstants.POSITIONS_POS_VALUE, INT)
                .build();
    }

    private IStoreDescription createVarStoreDescription() {
        return StoreDescription.builder()
                .withStoreName(StoreAndFieldConstants.VAR)
                .withField(StoreAndFieldConstants.VAR_ID, INT)
                .asKeyField()
                .withField(StoreAndFieldConstants.VAR_VALUES, INT)
                .build();
    }

    private IStoreDescription createCcpStoreDescription() {
        return StoreDescription.builder()
                .withStoreName(StoreAndFieldConstants.CCP)
                .withField(StoreAndFieldConstants.CCP_L4, STRING)
                .withField(StoreAndFieldConstants.CCP_L3, STRING)
                .withField(StoreAndFieldConstants.CCP_L2, STRING)
                .withField(StoreAndFieldConstants.CCP_L1, STRING)
                .withField(StoreAndFieldConstants.CCP_PID, INT)
                .asKeyField()
                .withField(StoreAndFieldConstants.CCP_VALUE, INT)
                .withField(StoreAndFieldConstants.CCP_AGG_LEVEL, STRING)
                .withField(StoreAndFieldConstants.CCP_AGG_LEVEL_ID, STRING)
                .build();
    }

    private IReferenceDescription positionsToCcpReference() {
        return StartBuilding.reference()
                .fromStore(StoreAndFieldConstants.POSITIONS)
                .toStore(StoreAndFieldConstants.CCP)
                .withName("PositionsToCcp")
                .withMapping(StoreAndFieldConstants.POSITIONS_ID, StoreAndFieldConstants.CCP_PID)
                .build();
    }

    private IReferenceDescription positionsToVarReference() {
        return StartBuilding.reference()
                .fromStore(StoreAndFieldConstants.POSITIONS)
                .toStore(StoreAndFieldConstants.VAR)
                .withName("PositionsToVar")
                .withMapping(StoreAndFieldConstants.POSITIONS_SECURITY_ID, StoreAndFieldConstants.VAR_ID)
                .build();
    }

    private Collection<IReferenceDescription> references() {
        final Collection<IReferenceDescription> references = new LinkedList<>();
        references.add(positionsToCcpReference());
        references.add(positionsToVarReference());

        return references;
    }

    @Override
    public IDatastoreSchemaDescription datastoreSchemaDescription() {
        final Collection<IStoreDescription> stores = new LinkedList<>();
        stores.add(createPositionsStoreDescription());
        stores.add(createVarStoreDescription());
        stores.add(createCcpStoreDescription());

        return new DatastoreSchemaDescription(stores, references());
    }
}

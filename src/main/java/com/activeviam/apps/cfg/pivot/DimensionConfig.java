/*
 * Copyright (C) ActiveViam 2023-2024
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.apps.cfg.pivot;

import org.springframework.context.annotation.Configuration;

import com.activeviam.apps.constants.StoreAndFieldConstants;
import com.activeviam.desc.build.ICanBuildCubeDescription;
import com.activeviam.desc.build.dimensions.ICanStartBuildingDimensions;
import com.quartetfs.biz.pivot.cube.hierarchy.ILevelInfo;
import com.quartetfs.biz.pivot.definitions.IActivePivotInstanceDescription;
import com.quartetfs.fwk.ordering.impl.ReverseOrderComparator;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Configuration
public class DimensionConfig {

    /**
     * Adds the dimensions descriptions to the input builder.
     *
     * @param builder
     *            The cube builder
     * @return The builder for chained calls
     */
    public ICanBuildCubeDescription<IActivePivotInstanceDescription> build(ICanStartBuildingDimensions builder) {
        return builder.withDimension(StoreAndFieldConstants.CCP_L1)
                .withHierarchy("Level")
                .slicing()
                .withLevels("L4", "L3", "L2", "L1")
                .withType(ILevelInfo.LevelType.REGULAR)
                .withComparator(ReverseOrderComparator.type);
    }
}

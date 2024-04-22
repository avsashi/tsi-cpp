/*
 * Copyright (C) ActiveViam 2023-2024
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.apps.cfg.pivot;

import static com.activeviam.apps.cfg.pivot.PivotManagerConfig.DOUBLE_FORMATTER;

import org.springframework.context.annotation.Configuration;

import com.activeviam.copper.ICopperContext;
import com.activeviam.copper.api.Copper;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Configuration
public class MeasureConfig {

    public void build(final ICopperContext context) {
                Copper.sum("CcpValue")
                        .as("Ccp Sum")
                        .withFormatter(DOUBLE_FORMATTER)
                        .publish(context);
    }
}

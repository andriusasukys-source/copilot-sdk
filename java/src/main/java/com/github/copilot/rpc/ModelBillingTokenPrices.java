/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *--------------------------------------------------------------------------------------------*/

package com.github.copilot.rpc;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Token-level pricing information for a model.
 *
 * @since 1.0.2
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ModelBillingTokenPrices {

    /**
     * AI Credits cost per billing batch of input tokens.
     */
    @JsonProperty("inputPrice")
    private Double inputPrice;

    /**
     * AI Credits cost per billing batch of output tokens.
     */
    @JsonProperty("outputPrice")
    private Double outputPrice;

    /**
     * AI Credits cost per billing batch of cached tokens.
     */
    @JsonProperty("cachePrice")
    private Double cachePrice;

    /**
     * Number of tokens per standard billing batch.
     */
    @JsonProperty("batchSize")
    private Integer batchSize;

    /**
     * Prompt token budget (max_prompt_tokens) for the default tier. The total
     * context window is this value plus the model's max_output_tokens.
     */
    @JsonProperty("contextMax")
    private Integer contextMax;

    /**
     * Long context tier pricing (available for models with extended context
     * windows).
     */
    @JsonProperty("longContext")
    private ModelBillingTokenPricesLongContext longContext;

    public Double getInputPrice() {
        return inputPrice;
    }

    public ModelBillingTokenPrices setInputPrice(Double inputPrice) {
        this.inputPrice = inputPrice;
        return this;
    }

    public Double getOutputPrice() {
        return outputPrice;
    }

    public ModelBillingTokenPrices setOutputPrice(Double outputPrice) {
        this.outputPrice = outputPrice;
        return this;
    }

    public Double getCachePrice() {
        return cachePrice;
    }

    public ModelBillingTokenPrices setCachePrice(Double cachePrice) {
        this.cachePrice = cachePrice;
        return this;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public ModelBillingTokenPrices setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public Integer getContextMax() {
        return contextMax;
    }

    public ModelBillingTokenPrices setContextMax(Integer contextMax) {
        this.contextMax = contextMax;
        return this;
    }

    public ModelBillingTokenPricesLongContext getLongContext() {
        return longContext;
    }

    public ModelBillingTokenPrices setLongContext(ModelBillingTokenPricesLongContext longContext) {
        this.longContext = longContext;
        return this;
    }
}

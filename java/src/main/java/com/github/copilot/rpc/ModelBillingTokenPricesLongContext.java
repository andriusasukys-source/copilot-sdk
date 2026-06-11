/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *--------------------------------------------------------------------------------------------*/

package com.github.copilot.rpc;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Long context tier pricing (available for models with extended context
 * windows).
 *
 * @since 1.0.2
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ModelBillingTokenPricesLongContext {

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
     * Prompt token budget (max_prompt_tokens) for the long context tier. The total
     * context window is this value plus the model's max_output_tokens.
     */
    @JsonProperty("contextMax")
    private Integer contextMax;

    public Double getInputPrice() {
        return inputPrice;
    }

    public ModelBillingTokenPricesLongContext setInputPrice(Double inputPrice) {
        this.inputPrice = inputPrice;
        return this;
    }

    public Double getOutputPrice() {
        return outputPrice;
    }

    public ModelBillingTokenPricesLongContext setOutputPrice(Double outputPrice) {
        this.outputPrice = outputPrice;
        return this;
    }

    public Double getCachePrice() {
        return cachePrice;
    }

    public ModelBillingTokenPricesLongContext setCachePrice(Double cachePrice) {
        this.cachePrice = cachePrice;
        return this;
    }

    public Integer getContextMax() {
        return contextMax;
    }

    public ModelBillingTokenPricesLongContext setContextMax(Integer contextMax) {
        this.contextMax = contextMax;
        return this;
    }
}

/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *--------------------------------------------------------------------------------------------*/

export function stripTrailingSlash(value: string): string {
    let end = value.length;
    while (end > 0 && value.charCodeAt(end - 1) === 47) {
        end--;
    }
    return end === value.length ? value : value.slice(0, end);
}

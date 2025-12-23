/**
 * Simple Promise-based fragment loader.
 * Fetches HTML fragments and injects them into the DOM containers.
 */
export const fragmentLoader = {
    /**
     * Loads a fragment into a container.
     * @param {string} containerId - The ID of the container element in the main document.
     * @param {string} fragmentPath - The path to the .html fragment file.
     * @returns {Promise<boolean>} - Resolves to true if loaded successfully.
     */
    load: async (containerId, fragmentPath) => {
        try {
            const response = await fetch(fragmentPath);
            if (!response.ok) {
                throw new Error(`Failed to load fragment from ${fragmentPath} (Status: ${response.status})`);
            }

            const html = await response.text();
            const container = document.getElementById(containerId);

            if (container) {
                container.innerHTML = html;
                return true;
            } else {
                console.warn(`[FragmentLoader] Container #${containerId} not found for ${fragmentPath}`);
                return false;
            }
        } catch (error) {
            console.warn(`[FragmentLoader] Error loading ${fragmentPath}:`, error);
            return false;
        }
    }
};

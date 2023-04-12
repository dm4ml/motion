<script>
    import { onMount } from "svelte";

    let query = "";
    let names = [];
    let results = [];

    onMount(async () => {
        const response = await fetch("/names");
        names = await response.json();
    });

    async function handleSearch() {
        const response = await fetch(`/timeline?query=${query}`);
        results = await response.json();
    }

    function handleSelect(name) {
        query = name;
        handleSearch();
    }
</script>

<div class="search-bar">
    <input
        type="text"
        placeholder="Search..."
        on:input={(e) => (query = e.target.value)}
        on:keydown={(event) => {
            if (event.key === "Enter") {
                const firstItem = names.filter((name) =>
                    name.toLowerCase().includes(query.toLowerCase())
                )[0];
                if (firstItem) {
                    handleSelect(firstItem);
                }
            }
        }}
    />
    {#if query}
        <ul class="dropdown">
            {#each names.filter((name) => name
                    .toLowerCase()
                    .includes(query.toLowerCase())) as name}
                <li
                    on:click={() => handleSelect(name)}
                    on:keydown={(event) => {
                        if (event.key === "Enter") {
                            handleSelect(name);
                        }
                    }}
                >
                    {name}
                </li>
            {/each}
        </ul>
    {/if}
</div>

<div class="timeline">
    {#if results.length > 0}
        <ul>
            {#each results as result}
                <li>{result}</li>
            {/each}
        </ul>
    {:else}
        <p>No results found.</p>
    {/if}
</div>

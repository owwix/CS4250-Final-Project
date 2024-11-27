'''
CRAWLER PSEUDOCDOE

procedure crawlerThread (frontier, num_targets)
    targets_found = 0
    while not frontier.done() do
        url <— frontier.nextURL()
        html <— retrieveURL(url)
    storePage(url, html)
    if target_page (parse (html))
        targets_found = targets_found + 1
    if targets_found = num_targets
        clear_frontier()
    else
        for each not visited url in parse (html) do
            frontier.addURL(url)
        end for
   end while
end procedure
'''


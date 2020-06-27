{
  "version": "0.3.2",
  "markups": [],
  "atoms": [],
  "cards": [
    ["hr", {}],
    {{#each fb_posts}}
        ["gallery",
               {
                {{#if has_locations}}
                    "caption": "{{{locations}}}",
                {{/if}}
                "images":
                    [
                    {{#each images}}
                        {"fileName": "{{filename}}","row":{{row}},"width":{{width}},"height":{{height}},"src":"{{src}}"}
                        {{#unless @last}},{{/unless}}
                    {{/each}}
                    ]
               }
         ]
        {{#unless @last}},{{/unless}}
    {{/each}}
  ],
  "sections" : [
     {{#each fb_posts}}
        [10,0],
        [1,"h1",[[0,[],0,"{{date}}"]]],
        {{#each places}}
            [1,"h3",[[0,[],0,"{{{name}}}"]]],
            [1, "h4", [ [0, [], 0, "{{{address}}}"] ]],
        {{/each}}
        {{#each tags}}
            [1, "p", [ [0, [], 0, "{{{this}}}"] ]],
        {{/each}}
        {{#each message}}
            [1, "p", [ [0, [], 0, "{{{this}}}"] ]],
        {{/each}}
        [1, "p", [ [0, [], 0, ""] ]]
        {{#if has_image}}
            ,[10, {{gallery_idx}}]
        {{/if}}
        {{#unless @last}},{{/unless}}
     {{/each}}
  ]
}
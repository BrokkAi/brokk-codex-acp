> ## Documentation Index
> Fetch the complete documentation index at: https://agentclientprotocol.com/llms.txt
> Use this file to discover all available pages before exploring further.

# Transports Working Group

> Announcing the new Transports Working Group, to stabilize new transport formats.

export const Author = ({name, role, github}) => <div className="mt-8 border-t border-gray-200 pt-4 dark:border-gray-800">
    <div className="font-semibold">
      {github ? <a href={github}>{name}</a> : name}
    </div>
    {role ? <div className="mt-1 text-sm opacity-80">{role}</div> : null}
  </div>;

**Published:** April 22, 2026

I'm excited to announce that we have a new Transports working group!

Remote Agent support is a key focus of ACP, and in order to make this more of a reality, we need to standardize all of the approaches to transports people have been trying.

We have started work on a Draft RFD for how this could work both via WebSockets and HTTP. Anna Zhdan will be representing from the Core Maintainers along with Alex Hancock from the Goose team who has been spearheading the RFD effort.

A big thanks to both of you for starting this and I look forward to seeing what is next!

<Author name="Ben Brandt" role="Zed Industries / ACP Lead Maintainer" github="https://github.com/benbrandt" />
